"""Restart-safe reconciliation for accepted training and inference operations."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from tributo.ray_jobs import get_ray_job_status, stop_ray_job

from tributo_broker_redis.active_jobs import (
    ActiveOperationRecord,
    ActiveOperationStore,
    TerminalCandidateStore,
)
from tributo_broker_redis.config import ChannelConfig, RedisBrokerConfig
from tributo_broker_redis.reporter import RedisEventReporter
from tributo_broker_redis.terminal_guard import TERMINAL_EVENT_TYPES, TerminalGuard

logger = logging.getLogger(__name__)


class ActiveOperationSupervisor:
    """Bounded dual-channel reconciliation backed only by Redis identities."""

    def __init__(
        self,
        redis_client: Any,
        config: RedisBrokerConfig,
        *,
        status_getter: Callable[..., str] = get_ray_job_status,
        stopper: Callable[..., bool] = stop_ray_job,
        wall_clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._redis = redis_client
        self._config = config
        self._active = ActiveOperationStore(redis_client, config.durability)
        self._candidates = TerminalCandidateStore(redis_client, config.durability)
        self._status_getter = status_getter
        self._stopper = stopper
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        self._next_check = 0.0
        self._closed = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def active_store(self) -> ActiveOperationStore:
        return self._active

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="tributo-redis-operation-supervisor",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._closed.wait(
            self._config.durability.supervisor_interval_seconds
        ):
            self.check_once()

    def check_due(self) -> None:
        now = self._monotonic()
        if now < self._next_check:
            return
        self._next_check = now + self._config.durability.supervisor_interval_seconds
        self.check_once()

    def check_once(self) -> None:
        try:
            records = tuple(self._active.scan())
        except Exception:
            logger.warning("Active operation scan failed", exc_info=True)
            return
        for record in records:
            try:
                self._reconcile(record)
            except Exception:
                logger.warning(
                    "Active operation reconciliation failed: operation_type=%s "
                    "operation_id=%s",
                    record.operation_type,
                    record.operation_id,
                    exc_info=True,
                )

    def _channel(self, record: ActiveOperationRecord) -> ChannelConfig:
        return self._config.channels.for_operation(record.operation_type)

    def _redis_hash_tag(self, record: ActiveOperationRecord) -> str | None:
        if self._config.transport.mode != "cluster":
            return None
        return f"{record.operation_type}:{record.operation_id}"

    def _guard(self, record: ActiveOperationRecord) -> TerminalGuard:
        return TerminalGuard(
            self._redis,
            outer_identity_field=self._channel(record).outer_identity_field,
            max_stream_length=self._config.transport.max_stream_length,
        )

    def _terminal(self, record: ActiveOperationRecord) -> dict[str, Any] | None:
        return self._guard(record).terminal_event(
            self._channel(record).event_stream_key(
                record.operation_id, redis_hash_tag=self._redis_hash_tag(record)
            ),
            record.operation_id,
        )

    def _cleanup(self, record: ActiveOperationRecord) -> None:
        self._active.delete(record)
        self._candidates.delete(record.operation_type, record.operation_id)

    def _replay_candidate(self, record: ActiveOperationRecord) -> bool:
        encoded = self._candidates.load(record.operation_type, record.operation_id)
        if encoded is None:
            return False
        value = json.loads(encoded)
        if not isinstance(value, dict):
            raise ValueError("terminal candidate must be an object")
        event_type = value.get("event_type")
        if event_type not in TERMINAL_EVENT_TYPES:
            raise ValueError("terminal candidate is not terminal")
        result = self._guard(record).publish(
            self._channel(record).event_stream_key(
                record.operation_id, redis_hash_tag=self._redis_hash_tag(record)
            ),
            operation_id=record.operation_id,
            encoded_event=encoded,
            event_type=str(event_type),
            phase=str(value.get("phase") or ""),
        )
        if result.accepted:
            self._cleanup(record)
        return True

    def _reporter(self, record: ActiveOperationRecord) -> RedisEventReporter:
        channel = self._channel(record)
        return RedisEventReporter(
            self._redis,
            event_stream_prefix=channel.event_stream_prefix,
            operation_id=record.operation_id,
            operation_type=record.operation_type,
            execution_profile=record.execution_profile,
            run_id=record.run_id,
            attempt_id=record.attempt_id,
            submission_id=record.submission_id,
            ray_job_id=record.ray_job_id,
            outer_identity_field=channel.outer_identity_field,
            max_event_bytes=self._config.transport.max_event_bytes,
            max_stream_length=self._config.transport.max_stream_length,
            durability_enabled=True,
            terminal_candidate_key_prefix=(
                self._config.durability.terminal_candidate_key_prefix
            ),
            terminal_candidate_ttl_seconds=(
                self._config.durability.terminal_candidate_ttl_seconds
            ),
            wire_protocol_profile=record.wire_protocol_profile,
            redis_hash_tag=self._redis_hash_tag(record),
        )

    def _publish_terminal(
        self, record: ActiveOperationRecord, event_type: str, payload: dict[str, Any]
    ) -> None:
        self._reporter(record).publish(event_type, payload, phase=event_type)
        self._cleanup(record)

    def _status(self, record: ActiveOperationRecord) -> str:
        try:
            return str(
                self._status_getter(
                    record.submission_id,
                    dashboard_url=self._config.execution.ray_dashboard_url,
                )
            ).upper()
        except LookupError:
            return "NOT_FOUND"
        except RuntimeError as exc:
            message = str(exc).lower()
            if any(
                marker in message
                for marker in ("404", "not found", "does not exist", "unknown job")
            ):
                return "NOT_FOUND"
            raise

    def _request_stop(self, record: ActiveOperationRecord, reason: str) -> None:
        stopped = self._stopper(
            record.submission_id,
            dashboard_url=self._config.execution.ray_dashboard_url,
        )
        if stopped:
            self._active.save(record.stopping(reason))
        else:
            self._active.refresh(record)

    def _reconcile(self, record: ActiveOperationRecord) -> None:
        if self._terminal(record) is not None:
            self._cleanup(record)
            return
        if self._replay_candidate(record):
            return

        status = self._status(record)
        if status in {"PENDING", "RUNNING"}:
            if record.stop_reason is not None:
                self._active.refresh(record)
                return
            if bool(
                self._redis.exists(
                    self._channel(record).cancel_key(
                        record.operation_id,
                        redis_hash_tag=self._redis_hash_tag(record),
                    )
                )
            ):
                self._request_stop(record, "CANCELLED")
                return
            if (
                record.deadline_at is not None
                and self._wall_clock() >= record.deadline_at
            ):
                self._request_stop(record, "TIMEOUT")
                return
            self._active.refresh(record)
            return

        if status == "STOPPED" and record.stop_reason == "CANCELLED":
            self._publish_terminal(
                record,
                "CANCELLED",
                {"reason": "cancel key observed after admission"},
            )
            return
        if status == "STOPPED" and record.stop_reason == "TIMEOUT":
            self._publish_terminal(
                record,
                "FAILED",
                {
                    "error_code": "OPERATION_TIMEOUT",
                    "sanitized_message": "operation exceeded its execution timeout",
                    "retryable": False,
                },
            )
            return
        if status in {"SUCCEEDED", "FAILED", "STOPPED", "NOT_FOUND"}:
            self._publish_terminal(
                record,
                "FAILED",
                {
                    "error_code": "TERMINAL_EVENT_MISSING",
                    "sanitized_message": (
                        "Ray operation ended without a durable terminal event"
                    ),
                    "retryable": False,
                    "ray_status": status,
                },
            )
            return
        self._active.refresh(record)

    def close(self) -> None:
        self._closed.set()
        if self._thread is not None:
            self._thread.join(
                timeout=max(
                    1.0,
                    self._config.durability.supervisor_interval_seconds * 2,
                )
            )


__all__ = ["ActiveOperationSupervisor"]
