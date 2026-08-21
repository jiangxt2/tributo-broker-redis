from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from tributo_broker_redis.capabilities import validate_supported_capabilities
from tributo_broker_redis.protocol_v2 import TrainingJobRequest


def canonical(datasource: dict[str, object]) -> TrainingJobRequest:
    value: dict[str, object] = {
        "protocol_version": "2.0",
        "job_id": "job-1",
        "model_id": "model-1",
        "version_id": "version-1",
        "tenant_id": "tenant-1",
        "algorithm": {"algorithm_key": "xgboost"},
        "datasource": datasource,
        "features": [
            {
                "feature_id": "f1",
                "result_column": "x",
                "column_type": "float",
            }
        ],
        "target": {
            "result_column": "label",
            "column_type": "int",
            "task_type": "BINARY_CLASSIFICATION",
        },
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
        "storage_context": {"type": "local", "prefix": "/tmp/bundles/"},
    }
    if str(datasource.get("type", "")).upper() in {"CLICKHOUSE", "HIVE"}:
        value["data_query"] = {"query": {"sql": "SELECT x, label FROM samples"}}
    return TrainingJobRequest.model_validate(value)


@pytest.mark.parametrize("source_type", ["S3", "LOCAL", "CLICKHOUSE", "HIVE"])
def test_declared_datasource_families_reach_capability_validation(
    source_type: str,
) -> None:
    properties: dict[str, object]
    if source_type == "S3":
        properties = {"uri": "s3://bucket/train.parquet"}
    elif source_type == "LOCAL":
        properties = {"path": "/data/train.csv", "format": "csv"}
    elif source_type == "HIVE":
        properties = {"auth": "NOSASL"}
    else:
        properties = {}
    datasource: dict[str, object] = {"type": source_type, "properties": properties}
    if source_type in {"CLICKHOUSE", "HIVE"}:
        datasource.update({"host": "db.internal", "database_name": "features"})
    request = canonical(datasource)
    validate_supported_capabilities(request)


@pytest.mark.parametrize("auth", ["LDAP", "CUSTOM", "KERBEROS"])
def test_hive_unresolved_auth_modes_fail_closed(auth: str) -> None:
    with pytest.raises(ValueError, match="datasource.properties.auth"):
        validate_supported_capabilities(
            canonical(
                {
                    "type": "HIVE",
                    "host": "hive.internal",
                    "database_name": "warehouse",
                    "properties": {"auth": auth},
                }
            )
        )


def test_inline_datasource_password_is_never_enabled() -> None:
    with pytest.raises(ValueError, match="datasource.password"):
        validate_supported_capabilities(
            canonical(
                {
                    "type": "CLICKHOUSE",
                    "host": "db.internal",
                    "database_name": "features",
                    "password": "opaque-secret",
                }
            )
        )


@pytest.mark.parametrize(
    ("field", "mutate"),
    [
        (
            "data_query.entity_key",
            lambda value: value["data_query"].update(
                {
                    "entity_key": {
                        "origin": {
                            "table_alias": "t",
                            "column_name": "id",
                            "column_type": "int",
                        },
                        "result_column": "id",
                    }
                }
            ),
        ),
        (
            "data_sampling.class_balance.strategy",
            lambda value: value.update(
                {
                    "data_sampling": {
                        "class_balance": {
                            "strategy": "CUSTOM_AMOUNT",
                            "class_amounts": {"0": 1, "1": 1},
                        }
                    }
                }
            ),
        ),
        (
            "data_split.stratify",
            lambda value: value.update({"data_split": {"stratify": True}}),
        ),
    ],
)
def test_removed_core_semantics_fail_closed(
    field: str, mutate: Callable[[dict[str, Any]], None]
) -> None:
    request = canonical(
        {"type": "CLICKHOUSE", "host": "db", "database_name": "features"}
    )
    value = request.model_dump(mode="python", exclude_unset=True)
    mutate(value)
    with pytest.raises(ValueError, match=field.replace(".", r"\.")):
        validate_supported_capabilities(TrainingJobRequest.model_validate(value))
