"""Redis-backed active-operation and terminal-candidate records."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import asdict, dataclass, replace
from itertools import islice
from typing import Any

from tributo_broker_redis.config import (
    DurabilityConfig,
    ExecutionProfile,
    OperationType,
)

logger = logging.getLogger(__name__)


def _cluster_primary_nodes(redis_client: Any) -> tuple[Any, ...]:
    get_nodes = getattr(redis_client, "get_nodes", None)
    if not callable(get_nodes):
        return ()
    return tuple(
        sorted(
            (
                node
                for node in get_nodes()
                if getattr(node, "server_type", "primary") == "primary"
            ),
            key=lambda node: str(node.name),
        )
    )


def _cluster_scan_cursor(raw_cursor: Any, node_name: str) -> int:
    if isinstance(raw_cursor, dict):
        if node_name not in raw_cursor:
            raise RuntimeError(
                f"Redis Cluster SCAN cursor map is missing node {node_name!r}"
            )
        raw_cursor = raw_cursor[node_name]
    if isinstance(raw_cursor, bool) or not isinstance(raw_cursor, (int, str, bytes)):
        raise RuntimeError(
            f"Redis Cluster SCAN returned an invalid cursor for node {node_name!r}"
        )
    try:
        cursor = int(raw_cursor)
    except (TypeError, ValueError) as error:
        raise RuntimeError(
            f"Redis Cluster SCAN returned an invalid cursor for node {node_name!r}"
        ) from error
    if cursor < 0:
        raise RuntimeError(
            f"Redis Cluster SCAN returned an invalid cursor for node {node_name!r}"
        )
    return cursor


_SAVE_ACTIVE_LUA = r"""
-- SAVE_ACTIVE_OPERATION
local raw = redis.call('GET', KEYS[1])
if raw then
    local ok, existing = pcall(cjson.decode, raw)
    local incoming_ok, incoming = pcall(cjson.decode, ARGV[1])
    if not ok or type(existing) ~= 'table' or not incoming_ok
        or type(incoming) ~= 'table' then
        return redis.error_reply('invalid active operation JSON')
    end
    if existing.submission_id ~= incoming.submission_id
        or existing.request_digest ~= incoming.request_digest then
        return redis.error_reply('active operation identity conflict')
    end
    if existing.stop_reason and existing.stop_reason ~= cjson.null then
        incoming.stop_reason = existing.stop_reason
    end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
"""

_STAGE_CANDIDATE_LUA = r"""
-- STAGE_TERMINAL_CANDIDATE
local raw = redis.call('GET', KEYS[1])
if raw then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    return raw
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return ARGV[1]
"""


def _decode(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


@dataclass(frozen=True)
class ActiveOperationRecord:
    operation_id: str
    operation_type: OperationType
    execution_profile: ExecutionProfile
    run_id: str
    attempt_id: str
    submission_id: str
    ray_job_id: str | None
    submitted_at: float
    deadline_at: float | None
    request_digest: str | None = None
    wire_protocol_profile: str = "tributo-generic-v1"
    stop_reason: str | None = None

    def encode(self) -> str:
        return json.dumps(
            asdict(self), separators=(",", ":"), sort_keys=True, allow_nan=False
        )

    @classmethod
    def decode(cls, raw: str | bytes) -> ActiveOperationRecord:
        value = json.loads(_decode(raw))
        if not isinstance(value, dict):
            raise ValueError("active operation record must be an object")
        return cls(**value)

    def stopping(self, reason: str) -> ActiveOperationRecord:
        return replace(self, stop_reason=reason)


class ActiveOperationStore:
    def __init__(self, redis_client: Any, durability: DurabilityConfig) -> None:
        self._redis = redis_client
        self._durability = durability

    def save(self, record: ActiveOperationRecord) -> None:
        result = self._redis.eval(
            _SAVE_ACTIVE_LUA,
            1,
            self._durability.active_key(record.operation_type, record.operation_id),
            record.encode(),
            str(self._durability.active_ttl_seconds),
        )
        if not result:
            raise RuntimeError("active operation was not persisted")

    def delete(self, record: ActiveOperationRecord) -> None:
        self._redis.delete(
            self._durability.active_key(record.operation_type, record.operation_id)
        )

    def get(
        self, operation_type: OperationType, operation_id: str
    ) -> ActiveOperationRecord | None:
        raw = self._redis.get(self._durability.active_key(operation_type, operation_id))
        return None if raw is None else ActiveOperationRecord.decode(raw)

    def refresh(self, record: ActiveOperationRecord) -> bool:
        return bool(
            self._redis.expire(
                self._durability.active_key(record.operation_type, record.operation_id),
                self._durability.active_ttl_seconds,
            )
        )

    def scan(self) -> Iterator[ActiveOperationRecord]:
        budget = self._durability.supervisor_scan_count
        scan = getattr(self._redis, "scan", None)
        if callable(scan):
            state_key = f"{self._durability.active_key_prefix}-scan-state"
            state_raw = self._redis.get(state_key)
            try:
                state = json.loads(_decode(state_raw)) if state_raw is not None else {}
            except (TypeError, json.JSONDecodeError):
                state = {}
            pending = list(state.get("pending", []))
            cluster_nodes = _cluster_primary_nodes(self._redis)
            if cluster_nodes:
                legacy_cursors = state.get("cursor", {})
                stored_cursors = state.get("node_cursors", legacy_cursors)
                node_cursors = (
                    {str(name): int(cursor) for name, cursor in stored_cursors.items()}
                    if isinstance(stored_cursors, dict)
                    else {}
                )
                node_index = int(state.get("node_index", 0)) % len(cluster_nodes)
                visited = 0
                while len(pending) < budget and visited < len(cluster_nodes):
                    node = cluster_nodes[node_index]
                    node_name = str(node.name)
                    next_cursor, discovered = scan(
                        cursor=node_cursors.get(node_name, 0),
                        match=f"{self._durability.active_key_prefix}:*",
                        count=budget - len(pending),
                        target_nodes=node,
                    )
                    node_cursors[node_name] = _cluster_scan_cursor(
                        next_cursor, node_name
                    )
                    pending.extend(_decode(key) for key in discovered)
                    node_index = (node_index + 1) % len(cluster_nodes)
                    visited += 1
                scan_state: dict[str, Any] = {
                    "node_cursors": node_cursors,
                    "node_index": node_index,
                }
            else:
                cursor = int(state.get("cursor", 0))
                while len(pending) < budget:
                    cursor, discovered = scan(
                        cursor=cursor,
                        match=f"{self._durability.active_key_prefix}:*",
                        count=budget - len(pending),
                    )
                    pending.extend(_decode(key) for key in discovered)
                    if cursor == 0 or pending:
                        break
                scan_state = {"cursor": cursor}
            keys = pending[:budget]
            self._redis.set(
                state_key,
                json.dumps(
                    {**scan_state, "pending": pending[budget:]},
                    separators=(",", ":"),
                ),
            )
        else:
            keys = list(
                islice(
                    self._redis.scan_iter(
                        match=f"{self._durability.active_key_prefix}:*",
                        count=budget,
                    ),
                    budget,
                )
            )
        for raw_key in keys:
            try:
                raw = self._redis.get(raw_key)
                if raw is not None:
                    yield ActiveOperationRecord.decode(raw)
            except Exception:
                logger.warning(
                    "Ignoring poisoned active operation record", exc_info=True
                )


class TerminalCandidateStore:
    """Persist the first terminal candidate before stream publication."""

    def __init__(self, redis_client: Any, durability: DurabilityConfig) -> None:
        self._redis = redis_client
        self._durability = durability

    def stage(
        self,
        operation_type: OperationType,
        operation_id: str,
        encoded_event: str,
    ) -> str:
        value = json.loads(encoded_event)
        if not isinstance(value, dict):
            raise ValueError("terminal candidate must be an object")
        candidate_identity = value.get("operation_id", value.get("job_id"))
        if candidate_identity != operation_id:
            raise ValueError("terminal candidate operation_id mismatch")
        candidate_type = value.get("operation_type", operation_type)
        if candidate_type != operation_type:
            raise ValueError("terminal candidate operation_type mismatch")
        if value.get("event_type") not in {"COMPLETED", "FAILED", "CANCELLED"}:
            raise ValueError("terminal candidate must be terminal")
        if value.get("protocol_version") == "2.0":
            from tributo_broker_redis.protocol_v2 import validate_terminal_event

            validate_terminal_event(value, operation_id)
        raw = self._redis.eval(
            _STAGE_CANDIDATE_LUA,
            1,
            self._durability.terminal_candidate_key(operation_type, operation_id),
            encoded_event,
            str(self._durability.terminal_candidate_ttl_seconds),
        )
        return _decode(raw)

    def load(self, operation_type: OperationType, operation_id: str) -> str | None:
        raw = self._redis.get(
            self._durability.terminal_candidate_key(operation_type, operation_id)
        )
        return None if raw is None else _decode(raw)

    def delete(self, operation_type: OperationType, operation_id: str) -> None:
        self._redis.delete(
            self._durability.terminal_candidate_key(operation_type, operation_id)
        )


def assert_cluster_safe_lua() -> None:
    """Durability scripts are single-key and operation keys share one hash tag."""
    for script in (_SAVE_ACTIVE_LUA, _STAGE_CANDIDATE_LUA):
        if "KEYS[2]" in script:
            raise AssertionError("durability Lua must use exactly one Redis key")
    durability = DurabilityConfig()
    active = durability.active_key("training", "job-1")
    candidate = durability.terminal_candidate_key("training", "job-1")
    if active[active.index("{") :] != candidate[candidate.index("{") :]:
        raise AssertionError("durability keys must share one Redis Cluster hash tag")


__all__ = [
    "ActiveOperationRecord",
    "ActiveOperationStore",
    "TerminalCandidateStore",
    "assert_cluster_safe_lua",
]
