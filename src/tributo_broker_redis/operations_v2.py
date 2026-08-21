"""Adapter from the strict KnoVa training protocol v2 to the generic driver."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

from pydantic import ValidationError

from tributo_broker_redis.capabilities import validate_supported_capabilities
from tributo_broker_redis.operations import MappingFailure, PreparedOperation
from tributo_broker_redis.protocol_v2 import (
    TrainingJobRequest,
    check_protocol_version,
    is_training_task,
)


@dataclass(frozen=True)
class PreparedV2Request:
    """Identity and payload normalized for the existing generic admission path."""

    operation_id: str
    execution_profile: Literal["single_worker", "distributed"]
    run_id: str
    attempt_id: str
    request_digest: str
    timeout_seconds: int | None
    prepared: PreparedOperation


def parse_and_prepare_v2_training(
    raw_payload: str,
    *,
    outer_operation_id: str,
    allow_legacy_training_config: bool,
) -> PreparedV2Request:
    """Strictly validate a v2 request and produce credential-free driver input."""
    try:
        value = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise MappingFailure("INVALID_JSON", "payload is not valid JSON") from exc
    if not isinstance(value, dict):
        raise MappingFailure("INVALID_REQUEST", "payload root must be an object")
    version_error = check_protocol_version(value)
    if version_error is not None:
        raise MappingFailure("UNSUPPORTED_PROTOCOL_VERSION", version_error)
    if not is_training_task(value):
        raise MappingFailure(
            "UNSUPPORTED_TASK_TYPE", "protocol v2 supports training tasks only"
        )
    embedded_job_id = value.get("job_id")
    if embedded_job_id not in (None, outer_operation_id):
        raise MappingFailure(
            "IDENTITY_MISMATCH", "outer operation identity does not match job_id"
        )
    value["job_id"] = outer_operation_id
    try:
        request = TrainingJobRequest.model_validate(value)
        if request.training_config is None:
            validate_supported_capabilities(request)
        config = request.resolve_training_config(
            allow_legacy_training_config=allow_legacy_training_config
        )
    except (ValidationError, ValueError) as exc:
        raise MappingFailure(
            "INVALID_TRAINING_REQUEST", "training request validation failed"
        ) from exc

    ray_config = config.get("ray", {})
    workers = ray_config.get("num_workers", 1) if isinstance(ray_config, dict) else 1
    execution_profile: Literal["single_worker", "distributed"] = (
        "distributed" if isinstance(workers, int) and workers >= 2 else "single_worker"
    )
    canonical_request = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    digest = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    completion_context = None
    if request.training_config is None:
        assert request.target is not None
        assert request.storage_context is not None
        completion_context = {
            "model_id": request.model_id,
            "version_id": request.version_id,
            "tenant_id": request.tenant_id,
            "algorithm_key": request.algorithm.algorithm_key,
            "task_type": request.target.task_type,
            "features": [
                {
                    "feature_id": feature.feature_id,
                    "result_column": feature.result_column,
                }
                for feature in request.features
            ],
            "evaluation": {
                "enabled": request.evaluation.enabled,
                "primary_metric": request.evaluation.primary_metric.lower(),
                "additional_metrics": tuple(
                    metric.lower() for metric in request.evaluation.additional_metrics
                ),
                "roc_curve": request.evaluation.artifacts.roc_curve,
                "threshold_analysis": (request.evaluation.artifacts.threshold_analysis),
                "confusion_matrix": request.evaluation.artifacts.confusion_matrix,
                "feature_importance": request.evaluation.artifacts.feature_importance,
            },
            "storage": request.storage_context.model_dump(mode="json"),
            "required_row_splits": tuple(
                name
                for name, ratio in (
                    ("train", request.data_split.train_ratio),
                    ("validation", request.data_split.validation_ratio),
                    ("test", request.data_split.test_ratio),
                )
                if ratio > 0
            ),
        }
    return PreparedV2Request(
        operation_id=outer_operation_id,
        execution_profile=execution_profile,
        run_id=outer_operation_id,
        attempt_id="attempt-1",
        request_digest=digest,
        timeout_seconds=request.resource_limits.max_training_time_seconds,
        prepared=PreparedOperation(
            operation_payload={
                "training_config": config,
                "v2_completion_context": completion_context,
            },
            credential_ref=None,
        ),
    )


__all__ = ["PreparedV2Request", "parse_and_prepare_v2_training"]
