from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from redis.cluster import ClusterNode
from redis.exceptions import (
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    CrossSlotTransactionError,
    ResponseError,
    SlotNotCoveredError,
)
from redis.sentinel import MasterNotFoundError

from tributo_broker_redis.config import (
    ChannelConfig,
    DurabilityConfig,
    RedisTransportConfig,
)
from tributo_broker_redis.execution_driver import _reporter
from tributo_broker_redis.protocol import DriverInput
from tributo_broker_redis.redis_client import (
    create_redis_client,
    is_transient_redis_error,
)


def test_topology_configuration_is_strict_and_credential_free() -> None:
    with pytest.raises(ValidationError, match="requires sentinel_urls"):
        RedisTransportConfig(mode="sentinel", sentinel_master_name="primary")
    with pytest.raises(ValidationError, match="requires cluster_urls"):
        RedisTransportConfig(mode="cluster")
    with pytest.raises(ValidationError, match="must not contain credentials"):
        RedisTransportConfig(url="redis://user:secret@redis:6379/0")
    with pytest.raises(ValidationError, match="database 0"):
        RedisTransportConfig(
            mode="cluster",
            cluster_urls=("redis://redis-1:6379/0",),
            database=1,
        )
    with pytest.raises(ValidationError, match="must match database"):
        RedisTransportConfig(url="redis://redis:6379/1", database=0)
    with pytest.raises(ValidationError, match="must match database"):
        RedisTransportConfig(
            url="redis://provider:6379/0",
            driver_url="redis://driver:6379/1",
            database=0,
        )
    with pytest.raises(ValidationError, match="must not contain a database path"):
        RedisTransportConfig(
            mode="sentinel",
            sentinel_urls=("redis://sentinel:26379/1",),
            sentinel_master_name="primary",
        )
    with pytest.raises(ValidationError, match="rejects Sentinel"):
        RedisTransportConfig(
            sentinel_username_env="SENTINEL_USER",
        )
    with pytest.raises(ValidationError, match="rejects Sentinel"):
        RedisTransportConfig(
            mode="cluster",
            cluster_urls=("redis://redis:7000/0",),
            sentinel_password_env="SENTINEL_PASSWORD",
        )


def test_standalone_resolves_credentials_only_from_preprovisioned_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = RedisTransportConfig(
        username_env="REDIS_RUNTIME_USER",
        password_env="REDIS_RUNTIME_PASSWORD",
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        create_redis_client(config)
    monkeypatch.setenv("REDIS_RUNTIME_USER", "service-user")
    monkeypatch.setenv("REDIS_RUNTIME_PASSWORD", "opaque-password")
    with patch("tributo_broker_redis.redis_client.redis.Redis.from_url") as create:
        create_redis_client(config)
    assert create.call_args.kwargs["username"] == "service-user"
    assert create.call_args.kwargs["password"] == "opaque-password"
    assert "opaque-password" not in config.model_dump_json()


def test_sentinel_rebuilds_master_and_sentinel_auth_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in {
        "MASTER_USER": "master-user",
        "MASTER_PASSWORD": "master-password",
        "SENTINEL_USER": "sentinel-user",
        "SENTINEL_PASSWORD": "sentinel-password",
    }.items():
        monkeypatch.setenv(name, value)
    config = RedisTransportConfig(
        mode="sentinel",
        sentinel_urls=("redis://sentinel-1:26379", "redis://sentinel-2:26379"),
        sentinel_master_name="primary",
        username_env="MASTER_USER",
        password_env="MASTER_PASSWORD",
        sentinel_username_env="SENTINEL_USER",
        sentinel_password_env="SENTINEL_PASSWORD",
    )
    sentinel = MagicMock()
    with patch(
        "tributo_broker_redis.redis_client.Sentinel", return_value=sentinel
    ) as cls:
        assert create_redis_client(config) is sentinel.master_for.return_value
    assert cls.call_args.args[0] == [("sentinel-1", 26379), ("sentinel-2", 26379)]
    assert cls.call_args.kwargs["sentinel_kwargs"]["username"] == "sentinel-user"
    assert sentinel.master_for.call_args.kwargs["username"] == "master-user"


def test_cluster_uses_typed_startup_nodes() -> None:
    config = RedisTransportConfig(
        mode="cluster",
        cluster_urls=("redis://redis-1:7000/0", "redis://redis-2:7001/0"),
    )
    with patch("tributo_broker_redis.redis_client.RedisCluster") as cls:
        create_redis_client(config)
    nodes = cls.call_args.kwargs["startup_nodes"]
    assert all(isinstance(node, ClusterNode) for node in nodes)
    assert [(node.host, node.port) for node in nodes] == [
        ("redis-1", 7000),
        ("redis-2", 7001),
    ]


def test_cluster_operation_keys_share_durability_hash_tag() -> None:
    channel = ChannelConfig(
        task_stream_key="tasks:training",
        event_stream_prefix="events:training",
        cancel_key_prefix="cancel:training",
        consumer_group="training",
    )
    durability = DurabilityConfig(enabled=True)
    tag = "training:job-1"
    keys = [
        channel.event_stream_key("job-1", redis_hash_tag=tag),
        channel.cancel_key("job-1", redis_hash_tag=tag),
        durability.active_key("training", "job-1"),
        durability.terminal_candidate_key("training", "job-1"),
    ]
    assert {key[key.index("{") : key.index("}") + 1] for key in keys} == {
        "{training:job-1}"
    }


@pytest.mark.parametrize(
    ("operation_type", "wire_profile"),
    [
        ("training", "tributo-generic-v1"),
        ("batch_inference", "tributo-generic-v1"),
        ("training", "knova-training-v2"),
    ],
)
def test_driver_reporter_preserves_cluster_hash_tag(
    operation_type: str, wire_profile: str
) -> None:
    value = DriverInput(
        operation_id="job-1",
        operation_type=operation_type,  # type: ignore[arg-type]
        execution_profile="distributed",
        run_id="job-1",
        attempt_id="attempt-1",
        operation_payload={},
        redis_url="redis://redis:6379/0",
        redis_hash_tag=f"{operation_type}:job-1",
        event_stream_prefix=f"events:{operation_type}",
        max_event_bytes=1024,
        max_stream_length=100,
        wire_protocol_profile=wire_profile,  # type: ignore[arg-type]
    )

    reporter = _reporter(value, MagicMock(), "submission-1")

    assert reporter.stream_key == f"events:{operation_type}:{{{operation_type}:job-1}}"


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError(),
        ClusterDownError("CLUSTERDOWN"),
        MasterNotFoundError(),
        ClusterError(),
        SlotNotCoveredError(),
    ],
)
def test_failover_and_connection_errors_are_transient(error: BaseException) -> None:
    assert is_transient_redis_error(error)
    assert not is_transient_redis_error(ValueError("bad configuration"))


@pytest.mark.parametrize(
    "error",
    [
        ClusterCrossSlotError("CROSSSLOT"),
        CrossSlotTransactionError(),
        ResponseError("ERR invalid command"),
    ],
)
def test_cross_slot_and_ordinary_response_errors_are_not_transient(
    error: BaseException,
) -> None:
    assert not is_transient_redis_error(error)
