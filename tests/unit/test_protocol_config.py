"""Unit tests for Redis provider protocol/config boundaries."""

from __future__ import annotations

import tomllib
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from redis.cluster import ClusterNode

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.protocol import (
    TrainingJobRequest,
    check_protocol_version,
    is_training_task,
)
from tributo_broker_redis.redis_client import create_redis_client


def test_protocol_version_and_training_scope() -> None:
    assert check_protocol_version({"protocol_version": "2.3"}) is None
    assert check_protocol_version({"protocol_version": "1.9"})
    assert is_training_task({}) is True
    assert is_training_task({"task_type": "INFERENCE"}) is False


def test_training_request_preserves_explicit_config() -> None:
    request = TrainingJobRequest(job_id="job-1", training_config={"data": {}})
    assert request.resolve_training_config() == {"data": {}}
    with pytest.raises(ValueError, match="non-empty"):
        TrainingJobRequest(job_id="job-1", training_config={}).resolve_training_config()


def test_canonical_clickhouse_request_maps_to_xgboost_bundle_config() -> None:
    request = TrainingJobRequest.model_validate(
        {
            "protocol_version": "2.0",
            "job_id": "train-job-1",
            "algorithm": {
                "algorithm_key": "xgboost",
                "hyper_params": {
                    "max_depth": 6,
                    "learning_rate": 0.08,
                    "n_estimators": 100,
                    "num_workers": 1,
                },
            },
            "datasource": {
                "type": "CLICKHOUSE",
                "host": "analytics.internal",
                "port": 9000,
                "database_name": "analytics",
                "username": "reader",
                "password": "***",
            },
            "data_query": {
                "query": {
                    "sql": "SELECT x1, x2, label FROM samples WHERE ts > {p0:String}",
                    "params": {"p0": "2026-01-01"},
                }
            },
            "features": [
                {"feature_id": "f1", "result_column": "x1"},
                {"feature_id": "f2", "result_column": "x2"},
            ],
            "target": {
                "result_column": "label",
                "task_type": "BINARY_CLASSIFICATION",
            },
            "data_split": {
                "train_ratio": 0.7,
                "validation_ratio": 0.1,
                "test_ratio": 0.2,
                "random_seed": 7,
            },
            "resource_limits": {"early_stopping_patience": 9},
            "storage_context": {
                "type": "s3",
                "bucket": "knova-models",
                "prefix": "tenant/model/version/",
            },
        }
    )

    config = request.resolve_training_config()

    assert config["data"]["type"] == "clickhouse"
    assert config["data"]["feature_columns"] == ["x1", "x2"]
    assert config["data"]["feature_id_map"] == {"x1": "f1", "x2": "f2"}
    assert config["model"]["eta"] == 0.08
    assert "learning_rate" not in config["model"]
    assert config["training"] == {
        "num_rounds": 100,
        "val_size": 0.1,
        "test_size": 0.2,
        "seed": 7,
        "early_stopping_rounds": 9,
    }
    assert config["ray"]["storage_path"] == (
        "s3://knova-models/tenant/model/version/_ray"
    )
    assert config["output"] == {"bundle_uri": "s3://knova-models/tenant/model/version"}


def test_canonical_request_requires_fields_needed_before_ray_submission() -> None:
    with pytest.raises(ValueError, match="datasource.host"):
        TrainingJobRequest.model_validate(
            {
                "job_id": "job-1",
                "features": [{"result_column": "x1"}],
                "target": {"result_column": "label"},
                "storage_context": {
                    "type": "local",
                    "prefix": "/tmp/bundles/",
                },
            }
        ).resolve_training_config()


def test_redis_config_supports_modes_and_rejects_missing_topology() -> None:
    first = RedisBrokerConfig()
    second = RedisBrokerConfig()
    assert first.mode == "standalone"
    assert first.consumer_name != second.consumer_name
    assert first.group_start_id == "$"
    assert (
        RedisBrokerConfig(
            mode="sentinel", sentinel_hosts=[("redis-sentinel", 26379)]
        ).mode
        == "sentinel"
    )
    with pytest.raises(ValidationError, match="cluster_startup_nodes"):
        RedisBrokerConfig(mode="cluster")
    with pytest.raises(ValidationError, match="requires db=0"):
        RedisBrokerConfig(
            mode="cluster",
            db=1,
            cluster_startup_nodes=[("redis-cluster", 6379)],
        )


def test_provider_limits_and_worker_specific_secret_reference() -> None:
    config = RedisBrokerConfig(
        max_payload_bytes=128,
        max_event_bytes=256,
        claim_count=25,
        worker_password_env="WORKER_REDIS_PASSWORD",
        extra_py_modules=["/provider/tributo_broker_redis"],
    )
    assert config.claim_count == 25
    assert config.worker_password_env == "WORKER_REDIS_PASSWORD"
    assert config.extra_py_modules == ["/provider/tributo_broker_redis"]


def test_redis_dependency_floor_supports_sentinel_force_master_ip() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    with pyproject.open("rb") as stream:
        dependencies = tomllib.load(stream)["project"]["dependencies"]
    assert "redis>=6.0,<8.0" in dependencies


def test_password_is_resolved_by_env_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_REDIS_PASSWORD", "not-in-config")
    config = RedisBrokerConfig(password_env="TEST_REDIS_PASSWORD")
    assert config.password() == "not-in-config"
    assert "not-in-config" not in config.model_dump_json()


def test_redis_url_rejects_embedded_credentials() -> None:
    with pytest.raises(ValidationError, match="must not contain credentials"):
        RedisBrokerConfig(url="redis://:embedded-secret@redis.example:6379")
    with pytest.raises(ValidationError, match="must not contain credentials"):
        RedisBrokerConfig(worker_url="redis://:embedded-secret@redis.example:6379")


def test_url_client_does_not_pass_false_ssl_to_redis_py() -> None:
    config = RedisBrokerConfig(url="redis://redis.example:6379")
    with patch("redis.Redis.from_url") as from_url:
        create_redis_client(config)
    assert from_url.call_args.kwargs == {"decode_responses": True}


def test_standalone_sentinel_and_cluster_clients_are_provider_owned() -> None:
    standalone = RedisBrokerConfig(host="redis", port=6380)
    sentinel = RedisBrokerConfig(mode="sentinel", sentinel_hosts=[("sentinel", 26379)])
    cluster = RedisBrokerConfig(
        mode="cluster",
        cluster_startup_nodes=[("node", 6379)],
        cluster_address_remap_host="127.0.0.1",
    )
    with (
        patch("redis.Redis") as redis_cls,
        patch("redis.Sentinel") as sentinel_cls,
        patch("redis.RedisCluster") as cluster_cls,
    ):
        create_redis_client(standalone)
        create_redis_client(sentinel)
        create_redis_client(cluster)
    redis_cls.assert_called_once_with(
        host="redis", port=6380, db=0, decode_responses=True
    )
    sentinel_cls.assert_called_once_with(
        [("sentinel", 26379)],
        decode_responses=True,
    )
    sentinel_cls.return_value.master_for.assert_called_once_with("mymaster", db=0)
    cluster_cls.assert_called_once_with(
        startup_nodes=[ClusterNode("node", 6379)],
        address_remap=cluster_cls.call_args.kwargs["address_remap"],
        decode_responses=True,
    )
    address_remap = cluster_cls.call_args.kwargs["address_remap"]
    assert address_remap(("10.0.0.2", 7001)) == ("127.0.0.1", 7001)


def test_sentinel_address_map_is_passed_to_provider_client() -> None:
    config = RedisBrokerConfig(
        mode="sentinel",
        sentinel_hosts=[("sentinel", 26379)],
        sentinel_address_map={"10.0.0.2:6379": ("127.0.0.1", 16381)},
    )
    with patch(
        "tributo_broker_redis.redis_client._AddressRemappingSentinel"
    ) as sentinel_cls:
        create_redis_client(config)
    sentinel_cls.assert_called_once_with(
        [("sentinel", 26379)],
        address_map={"10.0.0.2:6379": ("127.0.0.1", 16381)},
        decode_responses=True,
    )
