from __future__ import annotations

import base64
import json
import sys
from types import SimpleNamespace
from typing import Any

import pytest
from conftest import FakeRedis
from tributo.integrations.broker import Message
from tributo.ray_jobs import RayJobSubmission

import tributo_broker_redis.execution_driver as execution_driver
from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.runtime import RedisBrokerRuntime


def _request() -> dict[str, Any]:
    return {
        "protocol_version": "2.0",
        "job_id": "job-v2",
        "model_id": "model-1",
        "version_id": "version-1",
        "tenant_id": "tenant-1",
        "algorithm": {
            "algorithm_key": "xgboost",
            "hyper_params": {"num_workers": 2, "num_rounds": 2},
        },
        "datasource": {
            "type": "HIVE",
            "host": "hive.internal",
            "database_name": "warehouse",
            "properties": {"auth": "NOSASL", "parallelism": 2},
        },
        "data_query": {"query": {"sql": "SELECT x, label FROM samples"}},
        "feature_engineering": {
            "default_missing_value_strategy": "NONE",
            "default_outlier_strategy": "NONE",
            "default_scaling_method": "NONE",
            "default_encoding_method": "NONE",
        },
        "evaluation": {
            "artifacts": {
                "roc_curve": False,
                "threshold_analysis": False,
                "confusion_matrix": False,
                "feature_importance": False,
                "correlation_matrix": False,
            }
        },
        "features": [
            {"feature_id": "f1", "result_column": "x", "column_type": "float"}
        ],
        "target": {
            "result_column": "label",
            "column_type": "int",
            "task_type": "BINARY_CLASSIFICATION",
        },
        "storage_context": {"type": "local", "prefix": "/tmp/bundle/"},
    }


def _enabled(config: RedisBrokerConfig) -> RedisBrokerConfig:
    value = config.model_dump(mode="python")
    value["accept_knova_v2"] = True
    value["durability"] = {**value["durability"], "enabled": True}
    return RedisBrokerConfig.model_validate(value)


def test_v2_training_reuses_generic_driver_without_affecting_batch_channel(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    config = _enabled(config)
    calls: list[tuple[str, dict[str, Any]]] = []

    def submit(entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        calls.append((entrypoint, kwargs))
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id="submission-v2",
            ray_job_id="ray-v2",
            request_digest=kwargs["request_digest"],
        )

    fake_redis.messages[config.channels.training.task_stream_key] = [
        (
            "1-0",
            {
                "operation_id": "job-v2",
                "payload": json.dumps(_request()),
            },
        )
    ]
    runtime = RedisBrokerRuntime(config, submitter=submit, redis_client=fake_redis)

    assert runtime.run_once(timeout_ms=0) is True
    assert calls[0][0] == "python -m tributo_broker_redis.execution_driver"
    driver = json.loads(
        base64.urlsafe_b64decode(
            calls[0][1]["env_vars"]["TRIBUTO_REDIS_DRIVER_INPUT_B64"]
        )
    )
    assert driver["operation_type"] == "training"
    assert driver["execution_profile"] == "distributed"
    assert driver["wire_protocol_profile"] == "knova-training-v2"
    assert driver["operation_payload"]["v2_completion_context"]["model_id"] == (
        "model-1"
    )
    assert (
        driver["operation_payload"]["training_config"]["data"]["source"]["dialect"]
        == "hive"
    )
    context = calls[0][1]["execution_context"]
    assert context["cancellation"]["job_id"] == "job-v2"
    assert context["cancellation"]["options"]["cancel_key"] == (
        config.channels.training.cancel_key("job-v2")
    )
    assert context["event_reporter"]["factory_ref"].endswith(":create_event_reporter")
    events = [
        json.loads(item["payload"])
        for item in fake_redis.events[
            config.channels.training.event_stream_key("job-v2")
        ]
    ]
    assert [event["event_type"] for event in events] == ["PHASE", "ACCEPTED"]
    assert events[0]["phase"] == "QUEUED"
    assert "batch_inference" in runtime.consumers
    runtime.close()


def test_v2_redelivery_acks_without_second_ray_submission(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    config = _enabled(config)
    calls: list[dict[str, Any]] = []

    def submit(_entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        calls.append(kwargs)
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id="submission-v2",
            ray_job_id="ray-v2",
            request_digest=kwargs["request_digest"],
        )

    fields = {"operation_id": "job-v2", "payload": json.dumps(_request())}
    fake_redis.messages[config.channels.training.task_stream_key] = [
        ("1-0", fields),
        ("2-0", fields),
    ]
    runtime = RedisBrokerRuntime(config, submitter=submit, redis_client=fake_redis)

    assert runtime.run_once(timeout_ms=0) is True
    assert runtime.run_once(timeout_ms=0) is True

    assert len(calls) == 1
    assert [delivery for _stream, _group, delivery in fake_redis.acked] == [
        "1-0",
        "2-0",
    ]


def test_v2_semantically_identical_redelivery_has_stable_digest(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    config = _enabled(config)
    calls: list[dict[str, Any]] = []

    def submit(_entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        calls.append(kwargs)
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id="submission-v2",
            request_digest=kwargs["request_digest"],
        )

    request = _request()
    first = json.dumps(request, separators=(",", ":"))
    second = json.dumps(dict(reversed(tuple(request.items()))), indent=2)
    stream = config.channels.training.task_stream_key
    fake_redis.messages[stream] = [
        ("1-0", {"operation_id": "job-v2", "payload": first}),
        ("2-0", {"operation_id": "job-v2", "payload": second}),
    ]
    runtime = RedisBrokerRuntime(config, submitter=submit, redis_client=fake_redis)

    assert runtime.run_once(timeout_ms=0) is True
    assert runtime.run_once(timeout_ms=0) is True
    assert len(calls) == 1
    assert fake_redis.acked[-1][2] == "2-0"


def test_v2_terminal_redelivery_acks_without_active_record_or_submission(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    config = _enabled(config)
    calls: list[dict[str, Any]] = []

    def submit(_entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        calls.append(kwargs)
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id="submission-v2",
            ray_job_id="ray-v2",
            request_digest=kwargs["request_digest"],
        )

    stream = config.channels.training.task_stream_key
    fields = {"operation_id": "job-v2", "payload": json.dumps(_request())}
    fake_redis.messages[stream] = [("1-0", fields)]
    runtime = RedisBrokerRuntime(config, submitter=submit, redis_client=fake_redis)
    assert runtime.run_once(timeout_ms=0) is True
    fake_redis.delete(config.durability.active_key("training", "job-v2"))
    fake_redis.events[config.channels.training.event_stream_key("job-v2")].append(
        {
            config.channels.training.outer_identity_field: "job-v2",
            "payload": json.dumps({"protocol_version": "2.0", "event_type": "FAILED"}),
        }
    )
    fake_redis.messages[stream] = [("2-0", fields)]

    assert runtime.run_once(timeout_ms=0) is True
    assert len(calls) == 1
    assert fake_redis.acked[-1][2] == "2-0"


def test_v2_same_identity_different_payload_fails_closed_without_resubmit(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    config = _enabled(config)
    calls: list[dict[str, Any]] = []

    def submit(_entrypoint: str, **kwargs: Any) -> RayJobSubmission:
        calls.append(kwargs)
        return RayJobSubmission(
            run_id=kwargs["run_id"],
            attempt_id=kwargs["attempt_id"],
            submission_id="submission-v2",
            request_digest=kwargs["request_digest"],
        )

    stream = config.channels.training.task_stream_key
    fake_redis.messages[stream] = [
        ("1-0", {"operation_id": "job-v2", "payload": json.dumps(_request())})
    ]
    runtime = RedisBrokerRuntime(config, submitter=submit, redis_client=fake_redis)
    assert runtime.run_once(timeout_ms=0) is True

    changed = _request()
    changed["algorithm"]["hyper_params"]["num_rounds"] = 9
    fake_redis.messages[stream] = [
        ("2-0", {"operation_id": "job-v2", "payload": json.dumps(changed)})
    ]
    message = runtime.consumers["training"].poll(0)
    assert message is not None
    outcome = runtime.handle(message)

    assert outcome.disposition.value == "ack"
    assert outcome.error is not None
    assert outcome.error.code == "REQUEST_DIGEST_CONFLICT"
    assert len(calls) == 1


def test_dual_channel_consumer_does_not_starve_inference_and_recovers_both(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    runtime = RedisBrokerRuntime(config, redis_client=fake_redis)
    training = runtime.consumers["training"]
    inference = runtime.consumers["batch_inference"]
    training.recover_pending = lambda: 2  # type: ignore[method-assign]
    inference.recover_pending = lambda: 3  # type: ignore[method-assign]
    inference_message = Message(
        {"raw": "{}"},
        "inference-1",
        metadata={"operation_type": "batch_inference"},
    )

    def no_message(_timeout: int) -> Message | None:
        return None

    def one_message(_timeout: int) -> Message | None:
        return inference_message

    training.poll = no_message  # type: ignore[assignment]
    inference.poll = one_message  # type: ignore[assignment]

    assert runtime.consumer.recover_pending() == 5
    assert runtime.consumer.poll(0) is inference_message


def test_run_once_recovers_pending_on_both_channels_before_polling(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    runtime = RedisBrokerRuntime(config, redis_client=fake_redis)
    calls: list[str] = []

    def no_message(_timeout: int) -> Message | None:
        return None

    for operation_type, consumer in runtime.consumers.items():

        def recover(operation_type: str = operation_type) -> int:
            calls.append(operation_type)
            return 0

        consumer.recover_pending = recover  # type: ignore[method-assign]
        consumer.poll = no_message  # type: ignore[assignment]

    assert runtime.run_once(timeout_ms=0) is False
    assert sorted(calls) == ["batch_inference", "training"]


def test_runtime_maintain_drives_supervisor_tick(
    config: RedisBrokerConfig, fake_redis: FakeRedis
) -> None:
    runtime = RedisBrokerRuntime(config, redis_client=fake_redis)
    supervisor = SimpleNamespace(check_due=lambda: None, close=lambda: None)
    calls: list[str] = []
    supervisor.check_due = lambda: calls.append("tick")
    runtime._supervisor = supervisor  # type: ignore[assignment]

    runtime.maintain()

    assert calls == ["tick"]
    runtime.close()


def test_v2_driver_does_not_emit_generic_surrogate_phases_or_final_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tributo_broker_redis.protocol import DriverInput

    value = DriverInput(
        operation_id="job-v2",
        operation_type="training",
        execution_profile="distributed",
        run_id="job-v2",
        attempt_id="attempt-1",
        operation_payload={
            "training_config": {},
            "v2_completion_context": {"model_id": "model-1"},
        },
        redis_url="redis://redis:6379/0",
        event_stream_prefix="events:training",
        max_event_bytes=4096,
        max_stream_length=100,
        wire_protocol_profile="knova-training-v2",
    )
    result = SimpleNamespace(
        bundle_uri="/tmp/bundle",
        execution_id="execution-1",
        model_dump=lambda **_kwargs: {"metrics": {"eval_auc": 0.9}},
    )
    monkeypatch.setitem(
        sys.modules,
        "tributo.training.xgboost_trainer",
        SimpleNamespace(run_training_result_with_config=lambda _config: result),
    )
    monkeypatch.setattr(
        "tributo_broker_redis.completion_v2.build_v2_completed_payload",
        lambda *_args, **_kwargs: {
            "phase": "COMPLETED",
            "result_summary": {},
            "training_result": {},
            "artifact_manifest": {},
        },
    )
    events: list[tuple[str, str]] = []

    class Reporter:
        def publish(self, event_type, payload, *, phase):
            events.append((event_type, phase))

    assert execution_driver._run_training(value, Reporter()) == 0  # type: ignore[arg-type]
    assert events == [("LOG", "LOADING_DATA"), ("COMPLETED", "COMPLETED")]
