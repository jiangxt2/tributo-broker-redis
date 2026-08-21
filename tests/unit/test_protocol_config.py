from __future__ import annotations

import json
from typing import cast

import pytest
from conftest import FakeRedis
from pydantic import ValidationError

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol_v2 import (
    MAX_JOB_ID_LENGTH,
    MIN_TERMINAL_EVENT_BYTES,
    TrainingJobRequest,
    check_protocol_version,
    validate_terminal_event,
)
from tributo_broker_redis.reporter import RedisEventReporter


def test_v2_is_disabled_by_default_and_requires_durability(
    raw_config: dict[str, object],
) -> None:
    config = RedisBrokerConfig.model_validate(raw_config)
    assert config.accept_knova_v2 is False
    value = dict(raw_config)
    value["accept_knova_v2"] = True
    with pytest.raises(ValidationError, match="durability.enabled"):
        RedisBrokerConfig.model_validate(value)


def test_event_limit_must_fit_compact_emergency_terminal(
    raw_config: dict[str, object],
) -> None:
    value = dict(raw_config)
    transport = dict(cast(dict[str, object], value["transport"]))
    transport["max_event_bytes"] = MIN_TERMINAL_EVENT_BYTES - 1
    value["transport"] = transport
    with pytest.raises(ValidationError, match="greater than or equal"):
        RedisBrokerConfig.model_validate(value)


def test_minimum_event_limit_always_emits_valid_compact_v2_terminal() -> None:
    redis = FakeRedis()
    job_id = "j" * MAX_JOB_ID_LENGTH
    reporter = RedisEventReporter(
        redis,
        event_stream_prefix="events:training",
        operation_id=job_id,
        operation_type="training",
        execution_profile="distributed",
        run_id=job_id,
        attempt_id="attempt-1",
        outer_identity_field="job_id",
        max_event_bytes=MIN_TERMINAL_EVENT_BYTES,
        wire_protocol_profile="knova-training-v2",
    )

    event = reporter.publish(
        "COMPLETED",
        {
            "result_summary": {"large": "x" * 1000},
            "training_result": {},
            "artifact_manifest": {},
        },
        phase="COMPLETED",
    )

    assert event["event_type"] == "FAILED"
    assert len(json.dumps(event, separators=(",", ":"), sort_keys=True)) <= (
        MIN_TERMINAL_EVENT_BYTES
    )
    validate_terminal_event(event, job_id)


def test_v2_log_level_is_canonical_lowercase() -> None:
    redis = FakeRedis()
    reporter = RedisEventReporter(
        redis,
        event_stream_prefix="events:training",
        operation_id="job-1",
        operation_type="training",
        execution_profile="distributed",
        run_id="job-1",
        attempt_id="attempt-1",
        outer_identity_field="job_id",
        wire_protocol_profile="knova-training-v2",
    )

    event = reporter.publish("LOG", {"level": "INFO", "message": "started"})

    assert event["level"] == "info"


def test_v2_protocol_is_strict_and_bounds_job_identity() -> None:
    TrainingJobRequest(job_id="j" * MAX_JOB_ID_LENGTH, training_config={"data": {}})
    with pytest.raises(ValidationError, match="job_id"):
        TrainingJobRequest(
            job_id="j" * (MAX_JOB_ID_LENGTH + 1), training_config={"data": {}}
        )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        TrainingJobRequest.model_validate(
            {"job_id": "job-1", "training_config": {"data": {}}, "unknown": True}
        )


def test_v2_major_version_check_is_precise() -> None:
    assert check_protocol_version({"protocol_version": "2.9"}) is None
    assert "expected major version 2" in str(
        check_protocol_version({"protocol_version": "1.0"})
    )


def test_legacy_training_config_is_explicitly_gated() -> None:
    request = TrainingJobRequest(
        job_id="job-1",
        training_config={
            "data": {
                "source": {"type": "csv", "path": "/data/train.csv"},
                "label_col": "label",
            },
            "output": {"bundle_uri": "/tmp/bundle"},
        },
    )
    with pytest.raises(ValueError, match="legacy and disabled"):
        request.resolve_training_config()
    resolved = request.resolve_training_config(allow_legacy_training_config=True)
    assert resolved["data"]["source"]["type"] == "csv"


@pytest.mark.parametrize("key", ["password", "api_token", "private_key"])
def test_legacy_inline_secrets_fail_closed(key: str) -> None:
    request = TrainingJobRequest(
        job_id="job-1",
        training_config={
            "data": {"source": {"type": "csv", "path": "/data/train.csv"}},
            "model": {key: "opaque-secret"},
        },
    )
    with pytest.raises(ValueError, match="inline secret"):
        request.resolve_training_config(allow_legacy_training_config=True)
