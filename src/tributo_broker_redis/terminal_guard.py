"""Single-key Lua guard for ordered, globally unique terminal events."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

TERMINAL_EVENT_TYPES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})

_PUBLISH_EVENT_LUA = r"""
-- PUBLISH_GUARDED_EVENT
local stream_key = KEYS[1]
local identity_field = ARGV[1]
local operation_id = ARGV[2]
local encoded = ARGV[3]
local event_type = ARGV[4]
local phase = ARGV[5]
local max_length = ARGV[6]
local terminal = {COMPLETED=true, FAILED=true, CANCELLED=true}
local terminal_seen = false
local phase_seen = false

local entries = redis.call('XRANGE', stream_key, '-', '+')
for _, entry in ipairs(entries) do
    local fields = entry[2]
    local existing_identity = nil
    local existing_payload = nil
    for index = 1, #fields, 2 do
        if fields[index] == identity_field then existing_identity = fields[index + 1]
        elseif fields[index] == 'payload' then existing_payload = fields[index + 1] end
    end
    if existing_identity == operation_id and existing_payload ~= nil then
        local ok, existing = pcall(cjson.decode, existing_payload)
        if ok and type(existing) == 'table' and terminal[existing.event_type] then
            terminal_seen = true
        end
        if ok and type(existing) == 'table' and event_type == 'PHASE'
            and existing.event_type == 'PHASE' and existing.phase == phase then
            phase_seen = true
        end
    end
end

if terminal_seen then
    if terminal[event_type] then return {'terminal_exists', ''} end
    return {'rejected_after_terminal', ''}
end
if phase_seen then return {'duplicate_phase', ''} end
local event_id = redis.call(
    'XADD', stream_key, 'MAXLEN', '~', max_length, '*',
    identity_field, operation_id, 'payload', encoded
)
return {'published', event_id}
"""


class PublishDecision(StrEnum):
    PUBLISHED = "published"
    TERMINAL_EXISTS = "terminal_exists"
    REJECTED_AFTER_TERMINAL = "rejected_after_terminal"
    DUPLICATE_PHASE = "duplicate_phase"

    @property
    def accepted(self) -> bool:
        return self is not PublishDecision.REJECTED_AFTER_TERMINAL


@dataclass(frozen=True)
class GuardResult:
    decision: PublishDecision
    event_id: str | None = None

    @property
    def accepted(self) -> bool:
        return self.decision.accepted


def _decode(value: Any) -> Any:
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _fields(entry: Any) -> dict[str, Any]:
    if not isinstance(entry, (list, tuple)) or len(entry) < 2:
        return {}
    raw = entry[1]
    if isinstance(raw, dict):
        return {str(_decode(key)): _decode(value) for key, value in raw.items()}
    if not isinstance(raw, (list, tuple)):
        return {}
    return {
        str(_decode(raw[index])): _decode(raw[index + 1])
        for index in range(0, len(raw) - 1, 2)
    }


class TerminalGuard:
    def __init__(
        self,
        redis_client: Any,
        *,
        outer_identity_field: str,
        max_stream_length: int,
    ) -> None:
        self._redis = redis_client
        self._identity_field = outer_identity_field
        self._max_stream_length = max_stream_length

    def publish(
        self,
        stream_key: str,
        *,
        operation_id: str,
        encoded_event: str,
        event_type: str,
        phase: str | None,
    ) -> GuardResult:
        raw = self._redis.eval(
            _PUBLISH_EVENT_LUA,
            1,
            stream_key,
            self._identity_field,
            operation_id,
            encoded_event,
            event_type,
            phase or "",
            str(self._max_stream_length),
        )
        if not isinstance(raw, (list, tuple)) or not raw:
            raise RuntimeError(f"unexpected terminal guard response: {raw!r}")
        decision = PublishDecision(str(_decode(raw[0])))
        event_id = str(_decode(raw[1])) if len(raw) > 1 and raw[1] else None
        return GuardResult(decision=decision, event_id=event_id)

    def terminal_event(
        self, stream_key: str, operation_id: str
    ) -> dict[str, Any] | None:
        entries = self._redis.xrevrange(
            stream_key, max="+", min="-", count=self._max_stream_length
        )
        for entry in entries:
            fields = _fields(entry)
            if fields.get(self._identity_field) != operation_id:
                continue
            try:
                value = json.loads(fields.get("payload", ""))
            except (TypeError, json.JSONDecodeError):
                continue
            if (
                isinstance(value, dict)
                and value.get("event_type") in TERMINAL_EVENT_TYPES
            ):
                return value
        return None


def assert_single_key_lua() -> None:
    if "KEYS[2]" in _PUBLISH_EVENT_LUA:
        raise AssertionError("terminal guard Lua must use exactly one Redis key")


__all__ = [
    "GuardResult",
    "PublishDecision",
    "TERMINAL_EVENT_TYPES",
    "TerminalGuard",
    "assert_single_key_lua",
]
