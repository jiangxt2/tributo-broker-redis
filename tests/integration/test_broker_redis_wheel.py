"""Wheel-only entry-point discovery validation."""

from __future__ import annotations

from importlib.metadata import entry_points
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.integration


def test_installed_provider_is_discoverable_through_tributo_entry_points() -> None:
    matches = [
        entry
        for entry in entry_points(group="tributo.brokers")
        if entry.name == "tributo-redis"
    ]
    assert len(matches) == 1
    plugin_class = matches[0].load()
    assert plugin_class.broker_id == "tributo-redis"
    assert plugin_class.api_version == 1
    assert plugin_class.stability == "alpha"


def test_installed_core_accepts_provider_runtime_env_contract(
    tmp_path: Path,
) -> None:
    from tributo.ray_jobs import RayJobSubmission, submit_ray_job

    client = MagicMock()
    client.get_job_info.return_value = type(
        "JobInfo", (), {"job_id": "ray-core-job-contract"}
    )()
    extension_modules: list[str | Path] = [tmp_path / "provider-module"]
    runtime_packages = ["/provider/tributo_broker_redis-0.1.0-py3-none-any.whl"]
    runtime_env = {
        "py_modules": [str(tmp_path / "tributo"), *map(str, extension_modules)],
        "pip": list(runtime_packages),
    }

    with (
        patch("tributo.ray_jobs._get_submission_client", return_value=client),
        patch(
            "tributo.ray_jobs.build_runtime_env",
            autospec=True,
            return_value=runtime_env,
        ) as build_runtime_env_mock,
    ):
        result = submit_ray_job(
            "python -m tributo_broker_redis.execution_driver",
            operation_namespace="redis-training",
            run_id="wheel-contract-run",
            attempt_id="attempt-1",
            project_root=tmp_path,
            extra_py_modules=extension_modules,
            runtime_pip_packages=runtime_packages,
            request_digest="e" * 64,
        )

    assert isinstance(result, RayJobSubmission)
    build_call = build_runtime_env_mock.call_args.kwargs
    assert build_call["extra_py_modules"] == extension_modules
    assert build_call["runtime_pip_packages"] == runtime_packages
    assert build_call["env_vars"]["TRIBUTO_RUN_ID"] == "wheel-contract-run"
    assert build_call["env_vars"]["TRIBUTO_ATTEMPT_ID"] == "attempt-1"
    assert build_call["env_vars"]["TRIBUTO_SUBMISSION_ID"] == result.submission_id
    submit_call = client.submit_job.call_args.kwargs
    assert submit_call["runtime_env"] is runtime_env
    assert submit_call["metadata"] == {"tributo.request_digest": "e" * 64}
    assert result.ray_job_id == "ray-core-job-contract"
