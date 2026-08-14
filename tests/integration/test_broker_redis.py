"""Real Redis Streams and Docker Ray integration tests for the provider."""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis
from ray.job_submission import JobStatus, JobSubmissionClient
from tributo.integrations.broker import CancellationSpec, Message, TaskDisposition
from tributo.integrations.broker_runner import BrokerRunner, BrokerRunnerState

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.plugin import RedisBrokerPlugin

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture(scope="module")
def redis_client() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(
        os.environ.get("BROKER_REDIS_URL", "redis://127.0.0.1:16379"),
        decode_responses=True,
    )
    client.ping()
    yield client
    client.close()


@pytest.fixture(scope="module")
def provider_config() -> RedisBrokerConfig:
    suffix = uuid.uuid4().hex
    worker_url = os.environ.get(
        "BROKER_REDIS_WORKER_URL",
        "redis://host.docker.internal:16379",
    )
    wheel_name = os.environ.get("BROKER_PROVIDER_WHEEL_NAME")
    if not wheel_name:
        pytest.fail("BROKER_PROVIDER_WHEEL_NAME is required for wheel-only IT")
    project_root = os.environ.get("BROKER_PROJECT_ROOT")
    if not project_root:
        pytest.fail("BROKER_PROJECT_ROOT is required for Ray Jobs IT")
    return RedisBrokerConfig(
        url=os.environ.get("BROKER_REDIS_URL", "redis://127.0.0.1:16379"),
        worker_url=worker_url,
        task_stream_key=f"it:broker:tasks:{suffix}",
        event_stream_prefix=f"it:broker:events:{suffix}",
        cancel_key_prefix=f"it:broker:cancel:{suffix}",
        consumer_group=f"it-group-{suffix}",
        consumer_name=f"it-consumer-{suffix}",
        block_ms=100,
        claim_idle_ms=0,
        group_start_id="0-0",
        max_publish_retries=2,
        publish_retry_delay=0,
        ray_dashboard_url=os.environ.get(
            "BROKER_RAY_DASHBOARD_URL", "http://127.0.0.1:18265"
        ),
        project_root=project_root,
        runtime_pip_packages=[f"/provider/{wheel_name}"],
    )


def _events(client: redis.Redis, stream: str) -> list[dict[str, Any]]:
    payloads = []
    for _id, entry in client.xrange(stream):
        payload = entry.get("payload", entry.get(b"payload"))
        if isinstance(payload, bytes):
            payload = payload.decode()
        payloads.append(json.loads(payload))
    return payloads


def _task_payload(
    job_id: str,
    *,
    num_rounds: int = 2,
    val_size: float = 0.0,
) -> str:
    fixture_path = os.environ.get(
        "BROKER_TRAIN_FIXTURE",
        "/provider-data/broker_train.csv",
    )
    return json.dumps(
        {
            "protocol_version": "2.0",
            "task_type": "TRAINING",
            "job_id": job_id,
            "training_config": {
                "data": {
                    "type": "csv",
                    "format": "csv",
                    "path": fixture_path,
                    "label_col": "label",
                    "feature_columns": ["x1", "x2"],
                },
                "model": {"objective": "binary:logistic", "max_depth": 2},
                "training": {
                    "num_rounds": num_rounds,
                    "val_size": val_size,
                    "test_size": 0.0,
                },
                "ray": {"num_workers": 1, "storage_path": "/tmp/ray_results"},
                "output": {"onnx_path": "/tmp/tributo-broker-it.onnx"},
            },
        },
        separators=(",", ":"),
    )


def _run_one(plugin: RedisBrokerPlugin, config: RedisBrokerConfig) -> BrokerRunner:
    runner = BrokerRunner(plugin, config, poll_timeout_ms=100)
    assert runner.run_once() is True
    return runner


def _unused_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _wait_for_ray_job(
    dashboard_url: str,
    job_id: str,
    *,
    timeout: float = 240,
) -> JobStatus:
    client = JobSubmissionClient(dashboard_url)
    deadline = time.monotonic() + timeout
    status = JobStatus.PENDING
    while time.monotonic() < deadline:
        status = client.get_job_status(job_id)
        if status in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}:
            return status
        time.sleep(2)
    return status


def test_real_provider_unavailability_enters_reconnect_without_raising(
    provider_config: RedisBrokerConfig,
) -> None:
    """A real provider connection failure is contained by the Core runner."""
    unused_port = _unused_local_port()
    config = provider_config.model_copy(
        update={"url": f"redis://127.0.0.1:{unused_port}"}
    )
    runner = BrokerRunner(
        RedisBrokerPlugin(),
        config,
        poll_timeout_ms=0,
        backoff_initial=0.001,
        backoff_max=0.001,
        sleep=lambda _delay: None,
    )
    assert runner.run_once() is False
    assert runner.state == BrokerRunnerState.RECONNECTING
    runner.close()


def test_real_consumer_failure_after_start_enters_reconnect(
    provider_config: RedisBrokerConfig,
) -> None:
    """A real Redis operation failure after startup is isolated by Runner."""
    runner = BrokerRunner(
        RedisBrokerPlugin(),
        provider_config,
        poll_timeout_ms=0,
        backoff_initial=0.001,
        backoff_max=0.001,
        sleep=lambda _delay: None,
    )
    runner.start()
    assert runner.runtime is not None
    runner.runtime.consumer._redis = redis.Redis.from_url(
        f"redis://127.0.0.1:{_unused_local_port()}",
        decode_responses=True,
    )
    assert runner.run_once() is False
    assert runner.state == BrokerRunnerState.RECONNECTING
    runner.close()


def test_real_redis_invalid_message_failed_then_ack(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    plugin = RedisBrokerPlugin()
    runner = BrokerRunner(plugin, provider_config, poll_timeout_ms=100)
    redis_client.xadd(
        provider_config.task_stream_key,
        {"job_id": "invalid-job", "payload": "{not-json"},
    )
    assert runner.run_once() is True
    assert (
        redis_client.xpending(
            provider_config.task_stream_key, provider_config.consumer_group
        )["pending"]
        == 0
    )
    events = _events(redis_client, provider_config.event_stream_key("invalid-job"))
    assert events[-1]["event_type"] == "FAILED"
    runner.close()


def test_real_redis_missing_job_id_failed_then_ack(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    plugin = RedisBrokerPlugin()
    runner = BrokerRunner(plugin, provider_config, poll_timeout_ms=100)
    redis_client.xadd(
        provider_config.task_stream_key,
        {"payload": "{}"},
    )
    assert runner.run_once() is True
    assert (
        redis_client.xpending(
            provider_config.task_stream_key,
            provider_config.consumer_group,
        )["pending"]
        == 0
    )
    events = _events(redis_client, provider_config.invalid_event_stream_key)
    assert events[-1]["event_type"] == "FAILED"
    assert events[-1]["job_id"] is None
    assert events[-1]["error_code"] == "INVALID_JOB_ID"
    runner.close()


def test_real_pre_submission_cancellation_is_acked_without_ray_submission(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    job_id = f"it-cancel-queued-{uuid.uuid4().hex}"
    redis_client.set(provider_config.cancel_key(job_id), "1")
    redis_client.xadd(
        provider_config.task_stream_key,
        {"job_id": job_id, "payload": _task_payload(job_id)},
    )
    runner = BrokerRunner(RedisBrokerPlugin(), provider_config, poll_timeout_ms=100)
    try:
        assert runner.run_once() is True
        assert (
            redis_client.xpending(
                provider_config.task_stream_key,
                provider_config.consumer_group,
            )["pending"]
            == 0
        )
        events = _events(redis_client, provider_config.event_stream_key(job_id))
        assert events[-1]["event_type"] == "CANCELLED"
        assert events[-1]["phase"] == "QUEUED"
    finally:
        redis_client.delete(provider_config.cancel_key(job_id))
        runner.close()


def test_real_redis_pending_message_is_recovered_after_consumer_restart(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    """A pending delivery is claimable by the replacement consumer."""
    suffix = uuid.uuid4().hex
    stream = f"{provider_config.task_stream_key}:recovery:{suffix}"
    group = f"{provider_config.consumer_group}-recovery-{suffix}"
    first_config = provider_config.model_copy(
        update={
            "task_stream_key": stream,
            "consumer_group": group,
            "consumer_name": f"first-{suffix}",
            "claim_idle_ms": 0,
        }
    )
    redis_client.xadd(stream, {"job_id": "recovery-job", "payload": "{}"})

    first_client = redis.Redis.from_url(
        os.environ.get("BROKER_REDIS_URL", "redis://127.0.0.1:16379"),
        decode_responses=True,
    )
    second_client = redis.Redis.from_url(
        os.environ.get("BROKER_REDIS_URL", "redis://127.0.0.1:16379"),
        decode_responses=True,
    )
    try:
        first = RedisTaskConsumer(first_client, first_config)
        message = first.poll(100)
        assert message is not None
        assert message.job_id == "recovery-job"
        assert redis_client.xpending(stream, group)["pending"] == 1
        first.close()

        second_config = first_config.model_copy(
            update={"consumer_name": f"second-{suffix}"}
        )
        second = RedisTaskConsumer(second_client, second_config)
        assert second.recover_pending() == 1
        recovered = second.poll(100)
        assert recovered is not None
        assert recovered.job_id == "recovery-job"
        second.ack(recovered)
        assert redis_client.xpending(stream, group)["pending"] == 0
        second.close()
    finally:
        first_client.close()
        second_client.close()


def test_real_ack_failure_leaves_pending_for_recovery(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    suffix = uuid.uuid4().hex
    config = provider_config.model_copy(
        update={
            "task_stream_key": f"{provider_config.task_stream_key}:ack:{suffix}",
            "consumer_group": f"{provider_config.consumer_group}-ack-{suffix}",
            "consumer_name": f"ack-first-{suffix}",
            "group_start_id": "0-0",
            "claim_idle_ms": 0,
        }
    )
    redis_client.xadd(config.task_stream_key, {"job_id": "ack-job", "payload": "{}"})
    first = RedisTaskConsumer(redis_client, config)
    message = first.poll(100)
    assert message is not None
    first._redis = redis.Redis.from_url(
        f"redis://127.0.0.1:{_unused_local_port()}",
        decode_responses=True,
        socket_connect_timeout=0.1,
    )
    with pytest.raises(redis.RedisError):
        first.ack(message)
    assert (
        redis_client.xpending(config.task_stream_key, config.consumer_group)["pending"]
        == 1
    )

    second_config = config.model_copy(update={"consumer_name": f"ack-second-{suffix}"})
    second = RedisTaskConsumer(redis_client, second_config)
    try:
        assert second.recover_pending() == 1
        recovered = second.poll(100)
        assert recovered is not None
        second.ack(recovered)
        assert (
            redis_client.xpending(config.task_stream_key, config.consumer_group)[
                "pending"
            ]
            == 0
        )
    finally:
        first.close()
        second.close()


def test_real_redis_submission_ack_and_terminal_event(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    plugin = RedisBrokerPlugin()
    job_id = f"it-training-{uuid.uuid4().hex}"
    redis_client.xadd(
        provider_config.task_stream_key,
        {"job_id": job_id, "payload": _task_payload(job_id, val_size=0.5)},
    )
    runner = _run_one(plugin, provider_config)
    assert (
        redis_client.xpending(
            provider_config.task_stream_key, provider_config.consumer_group
        )["pending"]
        == 0
    )
    event_stream = provider_config.event_stream_key(job_id)
    deadline = time.monotonic() + 240
    event_types: set[str] = set()
    while time.monotonic() < deadline:
        events = _events(redis_client, event_stream)
        event_types = {event["event_type"] for event in events}
        if "COMPLETED" in event_types or "FAILED" in event_types:
            break
        time.sleep(2)
    assert {"PHASE", "LOG", "METRICS"}.issubset(event_types)
    assert "COMPLETED" in event_types, json.dumps(events, ensure_ascii=False)
    runner.close()


def test_real_duplicate_delivery_reconciles_same_ray_submission(
    provider_config: RedisBrokerConfig,
) -> None:
    """A redelivered task reuses the accepted Ray submission identity."""
    runtime = RedisBrokerPlugin().create_runtime(provider_config)
    job_id = f"it-idempotent-{uuid.uuid4().hex}"
    message = Message(
        job_id,
        {"raw": _task_payload(job_id)},
        delivery_id="duplicate-delivery",
        delivery_attempt=1,
    )
    try:
        first = runtime.handle(message)
        second = runtime.handle(
            Message(
                job_id,
                message.payload,
                delivery_id=message.delivery_id,
                delivery_attempt=2,
            )
        )
        assert first.disposition == TaskDisposition.ACK
        assert second.disposition == TaskDisposition.ACK
        assert first.result is not None
        assert second.result is not None
        assert first.result.submission_id == second.result.submission_id
        assert first.result.execution_id == second.result.execution_id
    finally:
        runtime.close()


def test_real_training_survives_unavailable_worker_event_redis(
    provider_config: RedisBrokerConfig,
) -> None:
    """Reporter loss in the Ray worker cannot turn training into failure."""
    config = provider_config.model_copy(
        update={"worker_url": f"redis://127.0.0.1:{_unused_local_port()}"}
    )
    runtime = RedisBrokerPlugin().create_runtime(config)
    job_id = f"it-reporter-down-{uuid.uuid4().hex}"
    try:
        outcome = runtime.handle(
            Message(job_id, {"raw": _task_payload(job_id, val_size=0.5)})
        )
        assert outcome.disposition == TaskDisposition.ACK
        assert outcome.result is not None
        assert outcome.result.execution_id is not None
        assert (
            _wait_for_ray_job(
                provider_config.ray_dashboard_url,
                outcome.result.execution_id,
            )
            == JobStatus.SUCCEEDED
        )
    finally:
        runtime.close()


def test_real_training_survives_redis_loss_during_execution(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    """Stopping Redis after submission does not fail the Ray training job."""
    job_id = f"it-training-redis-loss-{uuid.uuid4().hex}"
    runtime = RedisBrokerPlugin().create_runtime(provider_config)
    try:
        outcome = runtime.handle(
            Message(job_id, {"raw": _task_payload(job_id, num_rounds=10000)})
        )
        assert outcome.disposition == TaskDisposition.ACK
        assert outcome.result is not None
        assert outcome.result.execution_id is not None
        # Drop existing Redis client connections while the accepted Ray job
        # is executing.  The issuing test connection is excluded; the worker
        # reporter must tolerate the resulting connection failure and the
        # computation must still reach SUCCEEDED.
        redis_client.execute_command(
            "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"
        )
        assert (
            _wait_for_ray_job(
                provider_config.ray_dashboard_url,
                outcome.result.execution_id,
                timeout=240,
            )
            == JobStatus.SUCCEEDED
        )
    finally:
        runtime.close()


def test_real_training_cancellation_reports_cancelled(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
) -> None:
    job_id = f"it-cancel-training-{uuid.uuid4().hex}"
    runner = BrokerRunner(
        RedisBrokerPlugin(),
        provider_config.model_copy(update={"claim_idle_ms": 0}),
        poll_timeout_ms=100,
    )
    assert runner.run_once() is False
    redis_client.xadd(
        provider_config.task_stream_key,
        {"job_id": job_id, "payload": _task_payload(job_id, num_rounds=10000)},
    )
    try:
        assert runner.run_once() is True
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            events = _events(redis_client, provider_config.event_stream_key(job_id))
            if any(event.get("phase") == "TRAINING" for event in events):
                break
            time.sleep(0.5)
        redis_client.set(provider_config.cancel_key(job_id), "1")
        deadline = time.monotonic() + 240
        event_types: set[str] = set()
        while time.monotonic() < deadline:
            events = _events(redis_client, provider_config.event_stream_key(job_id))
            event_types = {event["event_type"] for event in events}
            if "CANCELLED" in event_types or "COMPLETED" in event_types:
                break
            time.sleep(2)
        assert "CANCELLED" in event_types, json.dumps(events, ensure_ascii=False)
    finally:
        runner.close()


def test_worker_checker_reconstructs_from_spec_and_observes_cancel(
    redis_client: redis.Redis,
    provider_config: RedisBrokerConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRIBUTO_BROKER_CONFIG_JSON", provider_config.model_dump_json())
    checker = RedisBrokerPlugin().create_cancellation_checker(
        CancellationSpec(
            broker_id="knova-redis",
            job_id="cancel-job",
            options={"config_env": "TRIBUTO_BROKER_CONFIG_JSON"},
        )
    )
    assert checker.is_cancelled("cancel-job") is False
    assert checker.is_cancelled("other-job") is False
    redis_client.set(provider_config.cancel_key("cancel-job"), "1")
    assert checker.is_cancelled("cancel-job") is True
