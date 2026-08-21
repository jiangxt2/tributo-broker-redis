"""Ray-worker controls reconstructed by Tributo's training execution SPI."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from tributo_broker_redis.config import ExecutionProfile, OperationType
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter


def _string(options: Mapping[str, Any], name: str) -> str:
    value = options.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"worker control option {name!r} must be a non-empty string")
    return value


class RedisCancellationChecker:
    """Check one exact Provider-owned cancellation key."""

    def __init__(self, job_id: str, options: Mapping[str, Any]) -> None:
        self._job_id = job_id
        self._cancel_key = _string(options, "cancel_key")
        self._redis = create_redis_client(_string(options, "redis_url"))

    def is_cancelled(self, job_id: str) -> bool:
        if job_id != self._job_id:
            raise ValueError("worker cancellation job identity mismatch")
        return bool(self._redis.exists(self._cancel_key))


class RedisTrainingEventReporter:
    """Translate Core's neutral phases/round metrics to protocol events."""

    def __init__(self, job_id: str, options: Mapping[str, Any]) -> None:
        self._job_id = job_id
        self._reporter = RedisEventReporter(
            create_redis_client(_string(options, "redis_url")),
            event_stream_prefix=_string(options, "event_stream_prefix"),
            operation_id=job_id,
            operation_type=cast(OperationType, _string(options, "operation_type")),
            execution_profile=cast(
                ExecutionProfile, _string(options, "execution_profile")
            ),
            run_id=_string(options, "run_id"),
            attempt_id=_string(options, "attempt_id"),
            outer_identity_field=_string(options, "outer_identity_field"),
            max_event_bytes=int(options.get("max_event_bytes", 1024 * 1024)),
            max_stream_length=int(options.get("max_stream_length", 1000)),
            durability_enabled=bool(options.get("durability_enabled", False)),
            terminal_candidate_key_prefix=_string(
                options, "terminal_candidate_key_prefix"
            ),
            terminal_candidate_ttl_seconds=int(
                options.get("terminal_candidate_ttl_seconds", 7 * 24 * 60 * 60)
            ),
            wire_protocol_profile=_string(options, "wire_protocol_profile"),
        )

    def _require_identity(self, job_id: str) -> None:
        if job_id != self._job_id:
            raise ValueError("worker reporter job identity mismatch")

    def report_phase(self, job_id: str, phase: str) -> None:
        self._require_identity(job_id)
        self._reporter.publish("PHASE", {"phase": phase}, phase=phase)

    def report_metrics(
        self,
        job_id: str,
        metrics: Mapping[str, Any],
        progress: float | None = None,
    ) -> None:
        self._require_identity(job_id)
        process_metrics: list[dict[str, Any]] = []
        for name, value in metrics.items():
            if (
                name == "round"
                or isinstance(value, bool)
                or not isinstance(value, (int, float))
            ):
                continue
            split, separator, metric_name = name.partition("-")
            item: dict[str, Any] = {
                "metric_name": metric_name if separator else name,
            }
            item["train" if split == "train" and separator else "eval"] = float(value)
            process_metrics.append(item)
        payload: dict[str, Any] = {
            "round": int(metrics["round"]) if "round" in metrics else None,
            "metrics": process_metrics,
        }
        if progress is not None:
            payload["progress_percent"] = min(100, max(0, round(progress * 100)))
        self._reporter.publish("METRICS", payload, phase="TRAINING")


def create_cancellation_checker(
    *, job_id: str, options: Mapping[str, Any]
) -> RedisCancellationChecker:
    """Factory referenced from ``TRIBUTO_EXECUTION_CONTEXT``."""
    return RedisCancellationChecker(job_id, options)


def create_event_reporter(
    *, job_id: str, options: Mapping[str, Any]
) -> RedisTrainingEventReporter:
    """Factory referenced from ``TRIBUTO_EXECUTION_CONTEXT``."""
    return RedisTrainingEventReporter(job_id, options)


__all__ = [
    "RedisCancellationChecker",
    "RedisTrainingEventReporter",
    "create_cancellation_checker",
    "create_event_reporter",
]
