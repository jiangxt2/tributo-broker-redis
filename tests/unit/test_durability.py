from __future__ import annotations

import fnmatch
import json
from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import pytest

from tributo_broker_redis.active_jobs import (
    ActiveOperationRecord,
    ActiveOperationStore,
    TerminalCandidateStore,
)
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.reporter import RedisEventReporter
from tributo_broker_redis.supervisor import ActiveOperationSupervisor
from tributo_broker_redis.terminal_guard import assert_single_key_lua


class DurableRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.events: dict[str, list[dict[str, str]]] = {}
        self.cancelled: set[str] = set()
        self.get_calls: list[str] = []

    def eval(self, script: str, _keys: int, key: str, *args: str) -> Any:
        if "SAVE_ACTIVE_OPERATION" in script:
            incoming = json.loads(args[0])
            if key in self.values:
                existing = json.loads(self.values[key])
                if existing["submission_id"] != incoming[
                    "submission_id"
                ] or existing.get("request_digest") != incoming.get("request_digest"):
                    raise RuntimeError("active operation identity conflict")
                if existing.get("stop_reason") is not None:
                    incoming["stop_reason"] = existing["stop_reason"]
            self.values[key] = json.dumps(incoming, separators=(",", ":"))
            return 1
        if "STAGE_TERMINAL_CANDIDATE" in script:
            self.values.setdefault(key, args[0])
            return self.values[key]
        if "PUBLISH_GUARDED_EVENT" in script:
            identity_field, operation_id, encoded, event_type, phase, _maxlen = args
            terminal = {"COMPLETED", "FAILED", "CANCELLED"}
            phase_seen = False
            for fields in self.events.get(key, []):
                if fields.get(identity_field) != operation_id:
                    continue
                event = json.loads(fields["payload"])
                if event.get("event_type") in terminal:
                    if event_type in terminal:
                        return ["terminal_exists", ""]
                    return ["rejected_after_terminal", ""]
                phase_seen |= (
                    event_type == "PHASE"
                    and event.get("event_type") == "PHASE"
                    and event.get("phase") == phase
                )
            if phase_seen:
                return ["duplicate_phase", ""]
            self.events.setdefault(key, []).append(
                {identity_field: operation_id, "payload": encoded}
            )
            return ["published", f"{len(self.events[key])}-0"]
        raise AssertionError("unexpected Lua script")

    def get(self, key: str) -> str | None:
        self.get_calls.append(key)
        return self.values.get(key)

    def set(self, key: str, value: str) -> bool:
        self.values[key] = value
        return True

    def delete(self, key: str) -> int:
        return int(self.values.pop(key, None) is not None)

    def expire(self, key: str, _ttl: int) -> int:
        return int(key in self.values)

    def scan_iter(self, *, match: str, count: int) -> Iterator[str]:
        del count
        return iter(key for key in tuple(self.values) if fnmatch.fnmatch(key, match))

    def xrevrange(
        self,
        stream: str,
        *,
        max: str = "+",  # noqa: A002
        min: str = "-",  # noqa: A002
        count: int,
    ) -> list[Any]:
        del max, min
        values = self.events.get(stream, [])[-count:]
        return [
            (f"{index}-0", fields)
            for index, fields in enumerate(reversed(values), start=1)
        ]

    def exists(self, key: str) -> int:
        return int(key in self.cancelled)


def durable_config(raw_config: dict[str, Any]) -> RedisBrokerConfig:
    value = dict(raw_config)
    value["durability"] = {
        "enabled": True,
        "supervisor_interval_seconds": 0.01,
        "default_timeout_seconds": 30,
    }
    return RedisBrokerConfig.model_validate(value)


def active(
    *, operation_id: str = "operation-1", operation_type: str = "training"
) -> ActiveOperationRecord:
    return ActiveOperationRecord(
        operation_id=operation_id,
        operation_type=operation_type,  # type: ignore[arg-type]
        execution_profile="distributed",
        run_id=f"run-{operation_id}",
        attempt_id="attempt-1",
        submission_id=f"submission-{operation_id}",
        ray_job_id=f"ray-{operation_id}",
        submitted_at=100.0,
        deadline_at=130.0,
    )


def reporter(redis: DurableRedis) -> RedisEventReporter:
    return RedisEventReporter(
        redis,
        event_stream_prefix="events:training",
        operation_id="operation-1",
        operation_type="training",
        execution_profile="distributed",
        run_id="run-operation-1",
        attempt_id="attempt-1",
        submission_id="submission-operation-1",
        durability_enabled=True,
    )


def test_lua_guard_allows_only_one_terminal_and_rejects_late_events() -> None:
    redis = DurableRedis()
    events = reporter(redis)

    events.publish("COMPLETED", {"result_reference": {"kind": "bundle"}})
    events.publish("FAILED", {"error_code": "LATE"})
    with pytest.raises(RuntimeError, match="after terminal"):
        events.publish("LOG", {"message": "late"})

    stream = redis.events["events:training:operation-1"]
    assert len(stream) == 1
    assert json.loads(stream[0]["payload"])["event_type"] == "COMPLETED"
    assert_single_key_lua()


def test_active_keys_do_not_collide_between_channels(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    store = ActiveOperationStore(redis, config.durability)
    store.save(active(operation_type="training"))
    store.save(active(operation_type="batch_inference"))

    assert len(tuple(store.scan())) == 2
    assert config.durability.active_key("training", "operation-1") != (
        config.durability.active_key("batch_inference", "operation-1")
    )


def test_active_save_rejects_same_submission_with_different_request_digest(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    store = ActiveOperationStore(redis, config.durability)
    record = replace(active(), request_digest="a" * 64)
    store.save(record)

    with pytest.raises(RuntimeError, match="identity conflict"):
        store.save(replace(record, request_digest="b" * 64))


def test_active_scan_bounds_examined_keys_and_isolates_poison_records(
    raw_config: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    config = durable_config(raw_config)
    durability = config.durability.model_copy(update={"supervisor_scan_count": 2})
    redis = DurableRedis()
    prefix = durability.active_key_prefix
    redis.values[f"{prefix}:0-poison"] = "not-json"
    valid = active(operation_id="valid")
    redis.values[f"{prefix}:1-valid"] = valid.encode()
    redis.values[f"{prefix}:2-unexamined"] = active(operation_id="unexamined").encode()

    records = tuple(ActiveOperationStore(redis, durability).scan())

    assert records == (valid,)
    assert len(redis.get_calls) == 2
    assert "poisoned active operation" in caplog.text


def test_active_scan_persists_cursor_and_covers_all_keys_across_ticks(
    raw_config: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    config = durable_config(raw_config)
    durability = config.durability.model_copy(update={"supervisor_scan_count": 2})

    class PagingRedis(DurableRedis):
        def scan(self, *, cursor: int, match: str, count: int) -> tuple[int, list[str]]:
            keys = sorted(key for key in self.values if fnmatch.fnmatch(key, match))
            page = keys[cursor : cursor + count]
            next_cursor = cursor + len(page)
            return (0 if next_cursor >= len(keys) else next_cursor), page

    redis = PagingRedis()
    prefix = durability.active_key_prefix
    expected = {f"operation-{index}" for index in range(5)}
    for operation_id in expected:
        record = active(operation_id=operation_id)
        redis.values[durability.active_key("training", operation_id)] = record.encode()
    redis.values[f"{prefix}:{{training:poison}}"] = "not-json"
    store = ActiveOperationStore(redis, durability)

    observed = {record.operation_id for _ in range(3) for record in tuple(store.scan())}

    assert observed == expected
    assert "poisoned active operation" in caplog.text


def test_supervisor_replays_candidate_before_ray_status(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    record = active()
    ActiveOperationStore(redis, config.durability).save(record)
    candidate = {
        "operation_id": record.operation_id,
        "operation_type": record.operation_type,
        "event_type": "COMPLETED",
        "phase": "COMPLETED",
    }
    TerminalCandidateStore(redis, config.durability).stage(
        record.operation_type,
        record.operation_id,
        json.dumps(candidate),
    )
    supervisor = ActiveOperationSupervisor(
        redis,
        config,
        status_getter=lambda *_args, **_kwargs: pytest.fail(
            "candidate replay must precede Ray status"
        ),
    )

    supervisor.check_once()

    stream = config.channels.training.event_stream_key(record.operation_id)
    assert json.loads(redis.events[stream][0]["payload"])["event_type"] == "COMPLETED"
    assert not redis.values


def test_cancel_is_terminal_only_after_ray_reports_stopped(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    record = active()
    store = ActiveOperationStore(redis, config.durability)
    store.save(record)
    redis.cancelled.add(config.channels.training.cancel_key(record.operation_id))
    statuses = iter(["RUNNING", "STOPPED"])
    stopped: list[str] = []

    def stop(submission_id: str, **_kwargs: Any) -> bool:
        stopped.append(submission_id)
        return True

    supervisor = ActiveOperationSupervisor(
        redis,
        config,
        status_getter=lambda *_args, **_kwargs: next(statuses),
        stopper=stop,
        wall_clock=lambda: 110.0,
    )

    supervisor.check_once()
    assert stopped == [record.submission_id]
    assert not redis.events
    supervisor.check_once()

    stream = config.channels.training.event_stream_key(record.operation_id)
    assert json.loads(redis.events[stream][0]["payload"])["event_type"] == "CANCELLED"
    assert not redis.values


def test_timeout_stops_then_publishes_stable_failure(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    record = active()
    ActiveOperationStore(redis, config.durability).save(record)
    statuses = iter(["RUNNING", "STOPPED"])
    supervisor = ActiveOperationSupervisor(
        redis,
        config,
        status_getter=lambda *_args, **_kwargs: next(statuses),
        stopper=lambda *_args, **_kwargs: True,
        wall_clock=lambda: 131.0,
    )

    supervisor.check_once()
    supervisor.check_once()

    stream = config.channels.training.event_stream_key(record.operation_id)
    event = json.loads(redis.events[stream][0]["payload"])
    assert event["event_type"] == "FAILED"
    assert event["payload"]["error_code"] == "OPERATION_TIMEOUT"
    assert not redis.values


def test_stop_false_retries_without_marking_operation_stopping(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    record = active()
    store = ActiveOperationStore(redis, config.durability)
    store.save(record)
    redis.cancelled.add(config.channels.training.cancel_key(record.operation_id))
    attempts: list[str] = []

    def fail_to_stop(submission_id: str, **_kwargs: Any) -> bool:
        attempts.append(submission_id)
        return False

    supervisor = ActiveOperationSupervisor(
        redis,
        config,
        status_getter=lambda *_args, **_kwargs: "RUNNING",
        stopper=fail_to_stop,
    )

    supervisor.check_once()
    supervisor.check_once()

    current = store.get("training", record.operation_id)
    assert attempts == [record.submission_id, record.submission_id]
    assert current is not None and current.stop_reason is None


def test_ray_runtime_404_is_normalized_to_controlled_terminal(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    record = active()
    ActiveOperationStore(redis, config.durability).save(record)

    def missing(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("Ray Jobs API returned HTTP 404")

    supervisor = ActiveOperationSupervisor(redis, config, status_getter=missing)
    supervisor.check_once()

    stream = config.channels.training.event_stream_key(record.operation_id)
    event = json.loads(redis.events[stream][0]["payload"])
    assert event["payload"]["ray_status"] == "NOT_FOUND"


def test_batch_inference_cancel_waits_for_stopped_and_is_unique(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    record = active(operation_type="batch_inference")
    ActiveOperationStore(redis, config.durability).save(record)
    redis.cancelled.add(config.channels.batch_inference.cancel_key(record.operation_id))
    statuses = iter(["RUNNING", "STOPPED"])
    supervisor = ActiveOperationSupervisor(
        redis,
        config,
        status_getter=lambda *_args, **_kwargs: next(statuses),
        stopper=lambda *_args, **_kwargs: True,
        wall_clock=lambda: 110.0,
    )

    supervisor.check_once()
    assert not redis.events
    supervisor.check_once()

    stream = config.channels.batch_inference.event_stream_key(record.operation_id)
    event = json.loads(redis.events[stream][0]["payload"])
    assert event["event_type"] == "CANCELLED"
    assert len(redis.events[stream]) == 1
    assert not redis.values


def test_batch_inference_completion_allows_only_one_terminal(
    raw_config: dict[str, Any],
) -> None:
    config = durable_config(raw_config)
    redis = DurableRedis()
    record = active(operation_type="batch_inference")
    channel = config.channels.batch_inference
    events = RedisEventReporter(
        redis,
        event_stream_prefix=channel.event_stream_prefix,
        operation_id=record.operation_id,
        operation_type="batch_inference",
        execution_profile=record.execution_profile,
        run_id=record.run_id,
        attempt_id=record.attempt_id,
        durability_enabled=True,
    )

    events.publish("COMPLETED", {"result": {"status": "succeeded"}})
    events.publish("FAILED", {"error_code": "LATE"})

    stream = channel.event_stream_key(record.operation_id)
    assert len(redis.events[stream]) == 1
    assert json.loads(redis.events[stream][0]["payload"])["event_type"] == "COMPLETED"
