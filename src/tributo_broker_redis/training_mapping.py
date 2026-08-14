"""Map KnoVa protocol v2 training requests to Tributo Core configuration."""

from __future__ import annotations

from typing import Any

from tributo_broker_redis.protocol import TrainingJobRequest

_CONTROL_HYPERPARAMS = {
    "early_stopping_rounds",
    "eta",
    "learning_rate",
    "max_failures",
    "n_estimators",
    "num_class",
    "num_rounds",
    "num_workers",
    "objective",
    "seed",
    "use_gpu",
}


def _required(value: Any, name: str) -> Any:
    if value is None or value == "" or value == []:
        raise ValueError(f"{name} is required for canonical training requests")
    return value


def _data_config(request: TrainingJobRequest) -> dict[str, Any]:
    datasource = request.datasource
    data_type = datasource.type.upper()
    if data_type == "S3":
        properties = datasource.properties
        uri = _required(properties.get("uri"), "datasource.properties.uri")
        s3 = {
            key: properties[key]
            for key in (
                "region",
                "access_key_id",
                "secret_access_key",
                "endpoint",
            )
            if properties.get(key)
        }
        data: dict[str, Any] = {
            "type": "s3",
            "uri": uri,
            "format": properties.get("format", "parquet"),
            "s3": s3,
        }
    elif data_type == "CLICKHOUSE":
        query = request.data_query.query if request.data_query else None
        data = {
            "type": "clickhouse",
            "ch_host": _required(datasource.host, "datasource.host"),
            "ch_port": datasource.port,
            "ch_database": _required(
                datasource.database_name, "datasource.database_name"
            ),
            "ch_user": datasource.username or "default",
            "ch_password": datasource.password or "",
            "ch_sql": _required(query.sql if query else None, "data_query.query.sql"),
            "ch_sql_params": query.params if query else {},
        }
    elif data_type in {"CSV", "LOCAL"}:
        properties = datasource.properties
        data = {
            "type": "csv",
            "path": _required(properties.get("path"), "datasource.properties.path"),
            "format": properties.get("format", "csv"),
        }
    else:
        raise ValueError(f"Unsupported canonical datasource type: {datasource.type!r}")

    if request.target is None:
        raise ValueError("target is required for canonical training requests")
    feature_columns = [feature.result_column for feature in request.features]
    _required(feature_columns, "features")
    data["label_col"] = request.target.result_column
    data["feature_columns"] = feature_columns
    data["feature_id_map"] = {
        feature.result_column: feature.feature_id
        for feature in request.features
        if feature.feature_id
    }
    return data


def _objective(
    request: TrainingJobRequest,
    hyper_params: dict[str, Any],
) -> tuple[str, int | None]:
    if request.target is None:
        raise ValueError("target is required for canonical training requests")
    task_type = request.target.task_type.upper()
    if task_type == "BINARY_CLASSIFICATION":
        return str(hyper_params.get("objective", "binary:logistic")), None
    if task_type == "REGRESSION":
        return str(hyper_params.get("objective", "reg:squarederror")), None
    if task_type == "MULTICLASS_CLASSIFICATION":
        num_class = hyper_params.get("num_class")
        if num_class is None and request.target.label_mapping:
            num_class = len(request.target.label_mapping)
        if num_class is None:
            raise ValueError(
                "MULTICLASS_CLASSIFICATION requires num_class or label_mapping"
            )
        return str(hyper_params.get("objective", "multi:softprob")), int(num_class)
    raise ValueError(f"Unsupported canonical target task_type: {task_type!r}")


def _model_config(request: TrainingJobRequest) -> dict[str, Any]:
    hyper_params = dict(request.algorithm.hyper_params)
    objective, num_class = _objective(request, hyper_params)
    model = {
        key: value
        for key, value in hyper_params.items()
        if key not in _CONTROL_HYPERPARAMS
    }
    model["objective"] = objective
    if num_class is not None:
        model["num_class"] = num_class
    eta = hyper_params.get("eta", hyper_params.get("learning_rate"))
    if eta is not None:
        model["eta"] = eta
    return model


def _training_config(request: TrainingJobRequest) -> dict[str, Any]:
    hyper_params = request.algorithm.hyper_params
    split = request.data_split
    if split.validation_ratio + split.test_ratio >= 1.0:
        raise ValueError("validation_ratio + test_ratio must be less than 1")
    config: dict[str, Any] = {
        "num_rounds": hyper_params.get(
            "num_rounds", hyper_params.get("n_estimators", 100)
        ),
        "val_size": split.validation_ratio,
        "test_size": split.test_ratio,
        "seed": hyper_params.get(
            "seed", split.random_seed if split.random_seed is not None else 42
        ),
    }
    early_stopping = hyper_params.get(
        "early_stopping_rounds",
        request.resource_limits.early_stopping_patience,
    )
    if early_stopping is not None:
        config["early_stopping_rounds"] = early_stopping
    return config


def _ray_config(request: TrainingJobRequest) -> dict[str, Any]:
    hyper_params = request.algorithm.hyper_params
    return {
        "num_workers": hyper_params.get("num_workers", 2),
        "use_gpu": hyper_params.get("use_gpu", False),
        "max_failures": hyper_params.get("max_failures", 0),
        "storage_path": f"{_bundle_uri(request)}/_ray",
    }


def _bundle_uri(request: TrainingJobRequest) -> str:
    """Resolve the canonical storage context to a Core-compatible URI."""
    storage = request.storage_context
    if storage is None:
        raise ValueError("storage_context is required for canonical Bundle publication")
    storage_type = storage.type.upper()
    if storage_type == "S3":
        bucket = _required(storage.bucket, "storage_context.bucket")
        prefix = _required(storage.prefix, "storage_context.prefix").rstrip("/")
        bundle_uri = f"s3://{bucket}/{prefix}"
    elif storage_type in {"FILE", "FILESYSTEM", "LOCAL"}:
        bundle_uri = _required(storage.prefix, "storage_context.prefix").rstrip("/")
    else:
        raise ValueError(
            f"Unsupported canonical storage_context type: {storage.type!r}"
        )
    return bundle_uri


def _output_config(request: TrainingJobRequest) -> dict[str, Any]:
    return {"bundle_uri": _bundle_uri(request)}


def build_training_config_from_request(
    request: TrainingJobRequest,
) -> dict[str, Any]:
    """Derive a Core XGBoost configuration from canonical KnoVa fields."""
    algorithm_key = request.algorithm.algorithm_key.lower()
    if algorithm_key != "xgboost":
        raise ValueError(
            f"Unsupported canonical algorithm: {request.algorithm.algorithm_key!r}"
        )
    return {
        "data": _data_config(request),
        "model": _model_config(request),
        "training": _training_config(request),
        "ray": _ray_config(request),
        "output": _output_config(request),
    }


def resolve_training_config(request: TrainingJobRequest) -> dict[str, Any]:
    """Preserve the legacy override or validate a canonical derived config."""
    if request.training_config is not None:
        if not request.training_config:
            raise ValueError("training_config must be a non-empty object")
        config = dict(request.training_config)
    else:
        config = build_training_config_from_request(request)

    from tributo.training.xgboost_trainer import XGBoostTrainingConfig

    try:
        XGBoostTrainingConfig.model_validate(config)
    except Exception as exc:
        raise ValueError(f"Invalid Tributo XGBoost training config: {exc}") from exc
    return config
