"""Provider factories implement Core's neutral worker-control SPI."""

from __future__ import annotations

import json

import pytest
from conftest import FakeRedis

from tributo_broker_redis.worker_controls import (
    RedisCancellationChecker,
    RedisTrainingEventReporter,
)


def _options() -> dict[str, object]:
    return {
        "redis_url": "redis://redis:6379/0",
        "cancel_key": "cancel:training:job-v2",
        "event_stream_prefix": "events:training",
        "operation_type": "training",
        "execution_profile": "distributed",
        "run_id": "job-v2",
        "attempt_id": "attempt-1",
        "outer_identity_field": "operation_id",
        "max_event_bytes": 4096,
        "max_stream_length": 100,
        "durability_enabled": False,
        "terminal_candidate_key_prefix": "terminal-candidate",
        "terminal_candidate_ttl_seconds": 3600,
        "wire_protocol_profile": "knova-training-v2",
    }


def test_worker_cancellation_uses_exact_job_and_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeRedis()
    redis.cancelled.add("cancel:training:job-v2")
    monkeypatch.setattr(
        "tributo_broker_redis.worker_controls.create_redis_client", lambda _url: redis
    )
    checker = RedisCancellationChecker("job-v2", _options())

    assert checker.is_cancelled("job-v2") is True
    with pytest.raises(ValueError, match="identity mismatch"):
        checker.is_cancelled("another-job")


def test_rank_zero_reporter_maps_phase_and_round_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = FakeRedis()
    monkeypatch.setattr(
        "tributo_broker_redis.worker_controls.create_redis_client", lambda _url: redis
    )
    reporter = RedisTrainingEventReporter("job-v2", _options())

    reporter.report_phase("job-v2", "TRAINING")
    reporter.report_metrics(
        "job-v2",
        {"round": 2, "train-logloss": 0.2, "val-logloss": 0.3},
        0.5,
    )

    events = [
        json.loads(item["payload"]) for item in redis.events["events:training:job-v2"]
    ]
    assert events[0]["phase"] == "TRAINING"
    assert events[1]["event_type"] == "METRICS"
    assert events[1]["round"] == 2
    assert events[1]["progress_percent"] == 50
    assert events[1]["metrics"] == [
        {"metric_name": "logloss", "train": 0.2},
        {"eval": 0.3, "metric_name": "logloss"},
    ]
