"""Strict KnoVa v2 completion built from Core result and Bundle evidence."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CompletionFeature(_StrictModel):
    feature_id: str = Field(min_length=1)
    result_column: str = Field(min_length=1)


class CompletionEvaluation(_StrictModel):
    enabled: bool
    primary_metric: str
    additional_metrics: tuple[str, ...] = ()
    roc_curve: bool = False
    threshold_analysis: bool = False
    confusion_matrix: bool = False
    feature_importance: bool = False


class CompletionStorage(_StrictModel):
    type: str  # noqa: A003
    bucket: str = ""
    prefix: str


class V2CompletionContext(_StrictModel):
    model_id: str
    version_id: str
    tenant_id: str
    algorithm_key: str
    task_type: str
    features: tuple[CompletionFeature, ...]
    evaluation: CompletionEvaluation
    storage: CompletionStorage
    required_row_splits: tuple[str, ...] = ("train",)


_METRIC_KEYS = {
    "auc": "auc",
    "f1": "f1",
    "precision": "precision",
    "recall": "recall",
    "average_precision": "avg_precision",
    "rmse": "rmse",
    "mae": "mae",
    "r2": "r2",
}


def _finite(value: Any, path: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{path} must be finite")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{path} must be finite")
    return result


def _metric(metrics: Mapping[str, Any], name: str) -> float | None:
    suffix = _METRIC_KEYS[name]
    alternatives = [f"eval_{suffix}"]
    if name in {"f1", "precision", "recall"}:
        alternatives.append(f"eval_{suffix}_macro")
    alternatives.append(suffix)
    for key in alternatives:
        if metrics.get(key) is not None:
            return _finite(metrics[key], f"metrics.{key}")
    return None


def _row_counts(
    metrics: Mapping[str, Any], required_splits: tuple[str, ...]
) -> dict[str, int]:
    values: dict[str, int] = {}
    for output_name, alternatives in {
        "train": ("row_count_train",),
        "validation": ("row_count_val", "row_count_validation"),
        "test": ("row_count_test", "eval_test_rows"),
    }.items():
        raw = next((metrics[key] for key in alternatives if key in metrics), None)
        if raw is None:
            if output_name in required_splits:
                raise ValueError(
                    f"Core TrainingResult.metrics is missing required "
                    f"row_count_{output_name}"
                )
            values[output_name] = 0
            continue
        value = _finite(raw, f"result_summary.sample_rows.{output_name}")
        if value < 0 or not value.is_integer():
            raise ValueError(
                f"result_summary.sample_rows.{output_name} must be a "
                "non-negative integer"
            )
        values[output_name] = int(value)
    return {"total": sum(values.values()), **values}


def _storage_root(storage: CompletionStorage) -> str:
    kind = storage.type.upper()
    if kind == "S3":
        return f"s3://{storage.bucket}/{storage.prefix.strip('/')}/".replace(
            "//", "//", 1
        )
    if kind in {"LOCAL", "FILE", "FILESYSTEM"}:
        return f"{storage.prefix.rstrip('/')}/"
    raise ValueError(f"unsupported completion storage type {storage.type!r}")


def _artifact_manifest(
    context: V2CompletionContext,
    bundle_uri: str,
    *,
    reader_factory: Callable[[], Any],
) -> dict[str, Any]:
    reader = reader_factory()
    manifest, manifest_bytes = reader.read_manifest_with_bytes(bundle_uri)
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    root = _storage_root(context.storage)
    canonical_uri = str(manifest.canonical_uri).rstrip("/")
    if not canonical_uri.startswith(root):
        raise ValueError("Bundle canonical_uri is outside storage_context")

    alternatives: list[dict[str, Any]] = []
    total_size = 0
    for artifact in manifest.artifacts:
        if artifact.artifact_kind != "model":
            continue
        files: list[dict[str, Any]] = []
        # BundleReader is the authority for repository fetch, size/hash and
        # tree-digest verification.  Re-check files here as a defensive
        # boundary for custom Reader implementations passed by integrations.
        with reader.open_artifact(
            bundle_uri,
            artifact_name=artifact.name,
            manifest=manifest,
            manifest_bytes=manifest_bytes,
        ) as resolved:
            for file in artifact.files:
                materialized = resolved.path_for(file.relative_path)
                if not materialized.is_file():
                    raise ValueError(
                        f"Bundle artifact is missing: {file.relative_path!r}"
                    )
                actual_size = materialized.stat().st_size
                hasher = hashlib.sha256()
                with materialized.open("rb") as stream:
                    while chunk := stream.read(1 << 20):
                        hasher.update(chunk)
                digest = hasher.hexdigest()
                if actual_size != file.size_bytes or digest != file.sha256:
                    raise ValueError(
                        f"Bundle artifact integrity mismatch: {file.relative_path!r}"
                    )
                uri = f"{canonical_uri}/artifacts/{artifact.name}/{file.relative_path}"
                if not uri.startswith(root):
                    raise ValueError("Bundle artifact is outside storage_context")
                total_size += file.size_bytes
                files.append(
                    {
                        "path": uri.removeprefix(root),
                        "uri": uri,
                        "size": file.size_bytes,
                        "hash": f"sha256:{file.sha256}",
                        "metadata": {
                            "artifact_name": artifact.name,
                            "role": file.role,
                            "tree_digest": artifact.tree_digest,
                        },
                    }
                )
        alternatives.append({"format": artifact.format, "files": files})
    if not alternatives or not any(item["files"] for item in alternatives):
        raise ValueError("Bundle manifest contains no downloadable model artifact")
    return {
        "model_id": context.model_id,
        "version_id": context.version_id,
        "tenant_id": context.tenant_id,
        "created_at": manifest.created_at.isoformat(),
        "algorithm_key": context.algorithm_key,
        "storage": context.storage.model_dump(mode="json"),
        "model_artifacts": {
            "model_weights": {
                "comment": "Core Bundle verified model artifacts",
                "required_for_inference": True,
                "alternatives": alternatives,
            }
        },
        "total_size_bytes": total_size,
        "bundle_id": manifest.bundle_id,
        "bundle_uri": canonical_uri,
        "manifest_sha256": manifest_sha256,
        "roles": dict(manifest.roles),
    }


def _evaluation(
    context: V2CompletionContext,
    metrics: Mapping[str, Any],
    rows: Mapping[str, int],
) -> dict[str, Any] | None:
    requested = (
        context.evaluation.primary_metric,
        *context.evaluation.additional_metrics,
    )
    scalars: list[dict[str, Any]] = []
    if context.evaluation.enabled:
        for name in requested:
            value = _metric(metrics, name)
            if value is None:
                raise ValueError(f"requested evaluation metric {name!r} is missing")
            scalars.append({"metric_name": name, "value": value})

    details: dict[str, Any] = {}
    if context.evaluation.roc_curve:
        fpr = metrics.get("eval_roc_fpr")
        tpr = metrics.get("eval_roc_tpr")
        if not isinstance(fpr, list) or not isinstance(tpr, list):
            raise ValueError("requested ROC curve is missing")
        details["roc_curve"] = {"fpr": fpr, "tpr": tpr}
    if context.evaluation.threshold_analysis:
        fields = {
            "thresholds": "eval_thr_thresholds",
            "precision_values": "eval_thr_precision",
            "recall_values": "eval_thr_recall",
            "f1_values": "eval_thr_f1",
            "predicted_positive_rows": "eval_thr_predicted_positive",
        }
        if any(not isinstance(metrics.get(key), list) for key in fields.values()):
            raise ValueError("requested threshold analysis is missing")
        details["threshold_analysis"] = {
            output: metrics[source] for output, source in fields.items()
        }
    if context.evaluation.confusion_matrix:
        if isinstance(metrics.get("eval_cm"), list):
            details["confusion_matrix"] = {
                "labels": [str(value) for value in metrics.get("eval_cm_labels", [])],
                "matrix": metrics["eval_cm"],
            }
        elif all(f"eval_cm_{name}" in metrics for name in ("tp", "fp", "fn", "tn")):
            details["confusion_matrix"] = {
                name: int(
                    _finite(metrics[f"eval_cm_{name}"], f"metrics.eval_cm_{name}")
                )
                for name in ("tp", "fp", "fn", "tn")
            }
        else:
            raise ValueError("requested confusion matrix is missing")
    return (
        {
            "eval_type": context.task_type,
            "sample_rows": rows["test"],
            "metrics": scalars,
            "details": details,
        }
        if context.evaluation.enabled
        else None
    )


def _feature_analysis(
    context: V2CompletionContext, metrics: Mapping[str, Any]
) -> dict[str, Any] | None:
    if not context.evaluation.feature_importance:
        return None
    ranks = metrics.get("feat_imp_rank")
    names = metrics.get("feat_imp_name")
    scores = metrics.get("feat_imp_score")
    if not all(isinstance(value, list) for value in (ranks, names, scores)):
        raise ValueError("requested feature importance is missing")
    assert (
        isinstance(ranks, list) and isinstance(names, list) and isinstance(scores, list)
    )
    id_by_name = {
        feature.result_column: feature.feature_id for feature in context.features
    }
    ranking: list[dict[str, Any]] = []
    for index, name in enumerate(names):
        normalized = str(name)
        if normalized not in id_by_name:
            raise ValueError(
                f"feature importance name {normalized!r} has no feature_id"
            )
        ranking.append(
            {
                "rank": int(ranks[index]),
                "feature_id": id_by_name[normalized],
                "model_feature_name": normalized,
                "importance_score": _finite(
                    scores[index], "training_result.feature_analysis.importance_score"
                ),
            }
        )
    return {"importance_ranking": ranking}


def build_v2_completed_payload(
    context_value: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    duration_seconds: float,
    reader_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Build the old observable v2 terminal only from verified Core evidence."""
    context = V2CompletionContext.model_validate(context_value)
    metrics = result.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ValueError("Core TrainingResult.metrics is required")
    bundle_uri = result.get("bundle_uri")
    if not isinstance(bundle_uri, str) or not bundle_uri:
        raise ValueError("Core TrainingResult.bundle_uri is required")
    if reader_factory is None:
        from tributo.exporting.bundle_reader import BundleReader

        reader_factory = BundleReader
    rows = _row_counts(metrics, context.required_row_splits)
    primary_value = (
        _metric(metrics, context.evaluation.primary_metric)
        if context.evaluation.enabled
        else None
    )
    if context.evaluation.enabled and primary_value is None:
        raise ValueError("requested primary evaluation metric is missing")
    return {
        "phase": "COMPLETED",
        "progress_percent": 100,
        "duration_seconds": round(duration_seconds, 3),
        "result_summary": {
            "primary_metric": (
                {
                    "name": context.evaluation.primary_metric,
                    "value": primary_value,
                }
                if primary_value is not None
                else None
            ),
            "sample_rows": rows,
        },
        "training_result": {
            "algorithm_key": context.algorithm_key,
            "task_type": context.task_type,
            "model_features": [
                {
                    "model_feature_index": index,
                    "model_feature_name": feature.result_column,
                    "feature_id": feature.feature_id,
                    "transformation": "PASSTHROUGH",
                }
                for index, feature in enumerate(context.features)
            ],
            "evaluation": _evaluation(context, metrics, rows),
            "feature_analysis": _feature_analysis(context, metrics),
            "tuning_result": None,
            "core_lifecycle": {
                key: result.get(key)
                for key in (
                    "training_status",
                    "bundle_status",
                    "hook_status",
                    "execution_id",
                )
            },
        },
        "artifact_manifest": _artifact_manifest(
            context, bundle_uri, reader_factory=reader_factory
        ),
        "warnings": [],
    }


__all__ = ["V2CompletionContext", "build_v2_completed_payload"]
