"""Best-effort generic event publication owned by the provider."""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any

from tributo_broker_redis.active_jobs import TerminalCandidateStore
from tributo_broker_redis.config import (
    DurabilityConfig,
    ExecutionProfile,
    OperationType,
)
from tributo_broker_redis.protocol import PROTOCOL_PROFILE, PROTOCOL_VERSION
from tributo_broker_redis.terminal_guard import TERMINAL_EVENT_TYPES, TerminalGuard

_SENSITIVE_KEY = re.compile(
    r"(?:password|passwd|secret|token|access[_-]?key|private[_-]?key)", re.IGNORECASE
)


def redact(value: Any) -> Any:
    """Recursively remove values carried by credential-like keys."""
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key == "credential_ref":
                result[str(key)] = str(item)
            elif _SENSITIVE_KEY.search(str(key)):
                result[str(key)] = "[REDACTED]"
            else:
                result[str(key)] = redact(item)
        return result
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


class RedisEventReporter:
    """Publish bounded events for one public operation identity."""

    def __init__(
        self,
        redis_client: Any,
        *,
        event_stream_prefix: str,
        operation_id: str,
        operation_type: OperationType,
        execution_profile: ExecutionProfile,
        run_id: str,
        attempt_id: str,
        submission_id: str | None = None,
        ray_job_id: str | None = None,
        outer_identity_field: str = "operation_id",
        max_event_bytes: int = 1024 * 1024,
        max_stream_length: int = 1000,
        durability_enabled: bool = False,
        terminal_candidate_key_prefix: str = "tributo:terminal-candidate",
        terminal_candidate_ttl_seconds: int = 7 * 24 * 60 * 60,
        wire_protocol_profile: str = "tributo-generic-v1",
        redis_hash_tag: str | None = None,
    ) -> None:
        self._redis = redis_client
        stream_identity = f"{{{redis_hash_tag}}}" if redis_hash_tag else operation_id
        self._stream = f"{event_stream_prefix}:{stream_identity}"
        self._outer_identity_field = outer_identity_field
        self._identity = {
            "operation_id": operation_id,
            "operation_type": operation_type,
            "execution_profile": execution_profile,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "submission_id": submission_id,
            "ray_job_id": ray_job_id,
        }
        self._max_event_bytes = max_event_bytes
        self._max_stream_length = max_stream_length
        self._operation_type = operation_type
        self._wire_protocol_profile = wire_protocol_profile
        self._durability = durability_enabled
        self._candidates: TerminalCandidateStore | None
        self._guard: TerminalGuard | None
        if durability_enabled:
            durability = DurabilityConfig(
                enabled=True,
                terminal_candidate_key_prefix=terminal_candidate_key_prefix,
                terminal_candidate_ttl_seconds=terminal_candidate_ttl_seconds,
            )
            self._candidates = TerminalCandidateStore(redis_client, durability)
            self._guard = TerminalGuard(
                redis_client,
                outer_identity_field=outer_identity_field,
                max_stream_length=max_stream_length,
            )
        else:
            self._candidates = None
            self._guard = None

    @property
    def stream_key(self) -> str:
        return self._stream

    def publish(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        phase: str | None = None,
    ) -> dict[str, Any]:
        if self._wire_protocol_profile == "knova-training-v2":
            canonical_payload = dict(redact(payload or {}))
            if event_type == "LOG" and isinstance(canonical_payload.get("level"), str):
                canonical_payload["level"] = canonical_payload["level"].lower()
            canonical_payload.setdefault("phase", phase)
            if event_type in TERMINAL_EVENT_TYPES:
                canonical_payload.setdefault("duration_seconds", 0.0)
            if event_type == "FAILED":
                canonical_payload.setdefault(
                    "error_message",
                    canonical_payload.pop("sanitized_message", "operation failed"),
                )
            if event_type == "CANCELLED":
                canonical_payload.setdefault("has_best_model", False)
            event = {
                "protocol_version": "2.0",
                "event_type": event_type,
                "job_id": self._identity["operation_id"],
                "timestamp": int(time.time() * 1000),
                **canonical_payload,
            }
        else:
            event = {
                "protocol_profile": PROTOCOL_PROFILE,
                "protocol_version": PROTOCOL_VERSION,
                "event_id": uuid.uuid4().hex,
                "timestamp_ms": int(time.time() * 1000),
                **self._identity,
                "event_type": event_type,
                "phase": phase,
                "payload": redact(payload or {}),
            }
        encoded = json.dumps(
            event,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
        if len(encoded.encode("utf-8")) > self._max_event_bytes:
            if event_type not in TERMINAL_EVENT_TYPES:
                raise ValueError("event exceeds configured size limit")
            event = self._compact_terminal_overflow(event_type)
            event_type = str(event["event_type"])
            phase = str(event.get("phase") or phase or "PUBLISHING")
            encoded = json.dumps(
                event, separators=(",", ":"), sort_keys=True, allow_nan=False
            )
            if len(encoded.encode("utf-8")) > self._max_event_bytes:
                raise ValueError("compact terminal exceeds configured size limit")
        if self._durability:
            assert self._guard is not None
            if event_type in TERMINAL_EVENT_TYPES:
                assert self._candidates is not None
                encoded = self._candidates.stage(
                    self._operation_type,
                    str(self._identity["operation_id"]),
                    encoded,
                )
            result = self._guard.publish(
                self._stream,
                operation_id=str(self._identity["operation_id"]),
                encoded_event=encoded,
                event_type=event_type,
                phase=phase,
            )
            if not result.accepted:
                raise RuntimeError("event rejected after terminal publication")
            if event_type in TERMINAL_EVENT_TYPES:
                assert self._candidates is not None
                self._candidates.delete(
                    self._operation_type, str(self._identity["operation_id"])
                )
        else:
            self._redis.xadd(
                self._stream,
                {
                    self._outer_identity_field: self._identity["operation_id"],
                    "payload": encoded,
                },
                maxlen=self._max_stream_length,
                approximate=True,
            )
        return event

    def _compact_terminal_overflow(self, original_event_type: str) -> dict[str, Any]:
        if self._wire_protocol_profile == "knova-training-v2":
            return {
                "protocol_version": "2.0",
                "event_type": "FAILED",
                "job_id": self._identity["operation_id"],
                "timestamp": int(time.time() * 1000),
                "phase": "PUBLISHING",
                "duration_seconds": 0.0,
                "error_code": "PAYLOAD_TOO_LARGE",
                "error_message": f"{original_event_type} event exceeded size limit",
            }
        return {
            "protocol_profile": PROTOCOL_PROFILE,
            "protocol_version": PROTOCOL_VERSION,
            "event_id": uuid.uuid4().hex,
            "timestamp_ms": int(time.time() * 1000),
            **self._identity,
            "event_type": "FAILED",
            "phase": "PUBLISHING",
            "payload": {
                "error_code": "PAYLOAD_TOO_LARGE",
                "sanitized_message": "terminal event exceeded size limit",
                "retryable": False,
            },
        }

    def phase(self, phase: str) -> dict[str, Any]:
        return self.publish("PHASE", {"phase": phase}, phase=phase)

    def log(self, message: str, level: str = "INFO") -> dict[str, Any]:
        return self.publish(
            "LOG",
            {"level": level, "message": message[:4096]},
        )

    def failed(self, code: str, error_type: str, phase: str) -> dict[str, Any]:
        if self._wire_protocol_profile == "knova-training-v2":
            return self.publish(
                "FAILED",
                {
                    "error_code": code,
                    "error_message": "operation failed",
                    "error_type": error_type,
                },
                phase=phase,
            )
        return self.publish(
            "FAILED",
            {
                "error_code": code,
                "error_type": error_type,
                "sanitized_message": "operation failed",
                "retryable": False,
            },
            phase=phase,
        )


__all__ = ["RedisEventReporter", "redact"]
