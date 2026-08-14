"""Unit tests for Redis Ray worker completion identity."""

from __future__ import annotations

import pytest

from tributo_broker_redis.run_training import (
    _result_from_summary,
    _worker_job_identity,
)


def test_worker_identity_falls_back_to_submission_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAY_JOB_ID", raising=False)
    monkeypatch.setenv("TRIBUTO_SUBMISSION_ID", "tributo-train-1")

    assert _worker_job_identity() == (
        "tributo-train-1",
        "tributo-train-1",
    )


def test_completion_result_preserves_both_ray_identities() -> None:
    result = _result_from_summary(
        "job-1",
        {"metrics": {"accuracy": 0.9}},
        run_id="job-1",
        attempt_id="attempt-1",
        execution_id="ray-job-1",
        submission_id="tributo-train-1",
    )

    assert result.execution_id == "ray-job-1"
    assert result.submission_id == "tributo-train-1"
