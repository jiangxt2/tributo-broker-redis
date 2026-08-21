from __future__ import annotations

import pytest

from tributo_broker_redis.protocol_v2 import TrainingJobRequest


def request(datasource: dict[str, object]) -> TrainingJobRequest:
    value: dict[str, object] = {
        "job_id": "job-1",
        "model_id": "model-1",
        "version_id": "version-1",
        "tenant_id": "tenant-1",
        "datasource": datasource,
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
    if str(datasource.get("type", "")).upper() in {"CLICKHOUSE", "HIVE"}:
        value["data_query"] = {
            "query": {
                "sql": "SELECT x, label FROM samples",
                "params": {"tenant": "acme"},
            }
        }
    return TrainingJobRequest.model_validate(value)


def test_hive_maps_to_canonical_gateway_source_without_loss() -> None:
    config = request(
        {
            "type": "HIVE",
            "host": "hive.internal",
            "database_name": "warehouse",
            "username": "reader",
            "properties": {
                "auth": "nosasl",
                "batch_size": 512,
                "shard_mode": "hash",
                "hash_column": "entity_id",
                "hash_shards": 8,
                "parallelism": 4,
            },
        }
    ).resolve_training_config()
    source = config["data"]["source"]
    assert source == {
        "type": "sql",
        "dialect": "hive",
        "host": "hive.internal",
        "port": 10000,
        "database": "warehouse",
        "user": "reader",
        "password": None,
        "auth": "NOSASL",
        "sql": "SELECT x, label FROM samples",
        "params": {"tenant": "acme"},
        "hash_shards": 8,
        "batch_size": 512,
        "parallelism": 4,
        "shard_mode": "hash",
        "hash_column": "entity_id",
    }


def test_clickhouse_maps_to_canonical_gateway_source() -> None:
    config = request(
        {
            "type": "CLICKHOUSE",
            "host": "ch.internal",
            "port": 8123,
            "database_name": "analytics",
            "properties": {"sort_key": "event_id", "parallelism": 3},
        }
    ).resolve_training_config()
    source = config["data"]["source"]
    assert source["dialect"] == "clickhouse"
    assert source["sort_key"] == "event_id"
    assert source["parallelism"] == 3


@pytest.mark.parametrize(
    ("datasource", "source_type"),
    [
        (
            {"type": "S3", "properties": {"uri": "s3://bucket/train.parquet"}},
            "parquet",
        ),
        (
            {
                "type": "LOCAL",
                "properties": {"path": "/data/train.csv", "format": "csv"},
            },
            "csv",
        ),
    ],
)
def test_file_sources_map_to_canonical_gateway(
    datasource: dict[str, object], source_type: str
) -> None:
    source = request(datasource).resolve_training_config()["data"]["source"]
    assert source["type"] == source_type
