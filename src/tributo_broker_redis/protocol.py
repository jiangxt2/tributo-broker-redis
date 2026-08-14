"""KnoVa training control-plane protocol models."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

PROTOCOL_VERSION = "2.0"


def check_protocol_version(value: dict[str, Any]) -> str | None:
    """Return a validation message when the protocol major is unsupported."""
    raw = value.get("protocol_version", "")
    if not isinstance(raw, str) or not raw:
        return "Missing protocol_version"
    if raw.split(".", 1)[0] != PROTOCOL_VERSION.split(".", 1)[0]:
        return (
            f"Unsupported protocol version: {raw!r} "
            f"(expected major version {PROTOCOL_VERSION.split('.', 1)[0]})"
        )
    return None


def is_training_task(value: dict[str, Any]) -> bool:
    """Return whether a request is in the v1 training scope."""
    task_type = value.get("task_type", value.get("job_type", "TRAINING"))
    return isinstance(task_type, str) and task_type.upper() in {
        "TRAINING",
        "TRAIN",
        "MODEL_TRAINING",
    }


class ProtocolModel(BaseModel):
    """Forward-compatible base for KnoVa protocol value objects."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class AlgorithmConfig(ProtocolModel):
    """KnoVa algorithm selection and engine-neutral hyperparameters."""

    category: str = "CLASSIFICATION"
    algorithm_key: str = "xgboost"
    hyper_params: dict[str, Any] = Field(default_factory=dict)


class DataSourceConfig(ProtocolModel):
    """KnoVa data-source connection and provider properties."""

    type: str = "CLICKHOUSE"  # noqa: A003
    datasource_id: str | None = None
    host: str = ""
    port: int = 9000
    database_name: str = ""
    username: str | None = None
    password: str | None = None
    credential_ref: str | None = None
    connection_string: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


class SqlTemplate(ProtocolModel):
    """Parameterized SQL query from the canonical request."""

    sql: str = ""
    params: dict[str, Any] = Field(default_factory=dict)


class DataQueryConfig(ProtocolModel):
    """Canonical training data-query plan."""

    mode: str = "DIRECT_QUERY"
    query: SqlTemplate | None = None


class FeatureSpec(ProtocolModel):
    """Canonical model feature, including legacy field aliases."""

    result_column: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("result_column", "physical_field"),
    )
    feature_id: str = ""


class TargetSpec(ProtocolModel):
    """Canonical prediction target, including legacy field aliases."""

    result_column: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("result_column", "physical_field"),
    )
    task_type: str = "BINARY_CLASSIFICATION"
    label_mapping: dict[str, int] | None = None


class DataSplitConfig(ProtocolModel):
    """Canonical train/validation/test split."""

    train_ratio: float = Field(default=0.7, gt=0.0, le=1.0)
    validation_ratio: float = Field(default=0.0, ge=0.0, lt=1.0)
    test_ratio: float = Field(default=0.3, ge=0.0, lt=1.0)
    random_seed: int | None = None


class ResourceLimits(ProtocolModel):
    """Canonical training-control limits used by the XGBoost adapter."""

    early_stopping_patience: int | None = Field(default=20, ge=1)


class StorageContext(ProtocolModel):
    """Canonical Bundle storage destination."""

    type: str = "s3"  # noqa: A003
    bucket: str = ""
    prefix: str = ""

    @field_validator("prefix")
    @classmethod
    def prefix_must_end_with_slash(cls, value: str) -> str:
        if value and not value.endswith("/"):
            raise ValueError("storage_context.prefix must end with '/'")
        return value


class TrainingJobRequest(ProtocolModel):
    """KnoVa v2 request with an optional legacy Tributo config override."""

    protocol_version: str = PROTOCOL_VERSION
    job_id: str = Field(..., min_length=1)
    model_id: str = ""
    version_id: str = ""
    tenant_id: str = ""
    algorithm: AlgorithmConfig = Field(default_factory=AlgorithmConfig)
    datasource: DataSourceConfig = Field(default_factory=DataSourceConfig)
    data_query: DataQueryConfig | None = None
    features: list[FeatureSpec] = Field(default_factory=list)
    target: TargetSpec | None = None
    data_split: DataSplitConfig = Field(default_factory=DataSplitConfig)
    resource_limits: ResourceLimits = Field(default_factory=ResourceLimits)
    storage_context: StorageContext | None = None
    training_config: dict[str, Any] | None = None

    def resolve_training_config(self) -> dict[str, Any]:
        """Resolve the legacy override or derive config from canonical fields."""
        from tributo_broker_redis.training_mapping import resolve_training_config

        return resolve_training_config(self)


def event_payload(
    *,
    job_id: str | None,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a JSON-safe event envelope for the KnoVa stream."""
    result = {
        "protocol_version": PROTOCOL_VERSION,
        "event_type": event_type,
        "job_id": job_id,
    }
    if payload:
        result.update(payload)
    return result
