from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import FakeRedis

from tributo_broker_redis.completion_v2 import _metric, build_v2_completed_payload
from tributo_broker_redis.reporter import RedisEventReporter


def context(root: Path) -> dict[str, object]:
    return {
        "model_id": "model-1",
        "version_id": "version-1",
        "tenant_id": "tenant-1",
        "algorithm_key": "xgboost",
        "task_type": "BINARY_CLASSIFICATION",
        "features": [{"feature_id": "feature-1", "result_column": "x"}],
        "evaluation": {
            "enabled": True,
            "primary_metric": "auc",
            "additional_metrics": ["f1"],
            "roc_curve": True,
            "threshold_analysis": True,
            "confusion_matrix": True,
            "feature_importance": True,
        },
        "storage": {"type": "local", "prefix": f"{root}/"},
    }


def result() -> dict[str, object]:
    return {
        "bundle_uri": "/requested/bundle/root",
        "execution_id": "execution-1",
        "training_status": "succeeded",
        "bundle_status": "succeeded",
        "hook_status": "not_configured",
        "metrics": {
            "row_count_train": 70.0,
            "row_count_val": 10.0,
            "row_count_test": 20.0,
            "eval_auc": 0.91,
            "eval_f1": 0.8,
            "eval_roc_fpr": [0.0, 1.0],
            "eval_roc_tpr": [0.0, 1.0],
            "eval_thr_thresholds": [0.5],
            "eval_thr_precision": [0.8],
            "eval_thr_recall": [0.7],
            "eval_thr_f1": [0.75],
            "eval_thr_predicted_positive": [12],
            "eval_cm_tp": 8,
            "eval_cm_fp": 1,
            "eval_cm_fn": 2,
            "eval_cm_tn": 9,
            "feat_imp_rank": [1],
            "feat_imp_name": ["x"],
            "feat_imp_score": [0.75],
        },
    }


def test_completed_terminal_maps_verified_bundle_and_downloadable_artifact(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle-id"
    artifact_path = bundle / "artifacts" / "onnx-model" / "model.onnx"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b"verified-model")
    digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    artifact_file = SimpleNamespace(
        relative_path="model.onnx",
        sha256=digest,
        size_bytes=artifact_path.stat().st_size,
        role="model",
    )
    artifact = SimpleNamespace(
        name="onnx-model",
        format="onnx",
        artifact_kind="model",
        tree_digest="a" * 64,
        files=(artifact_file,),
    )
    manifest = SimpleNamespace(
        canonical_uri=str(bundle),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        bundle_id="bundle-id",
        roles={"primary_model": "onnx-model"},
        artifacts=(artifact,),
    )
    resolved = SimpleNamespace(
        path_for=lambda relative_path: artifact_path.parent / relative_path
    )

    @contextmanager
    def open_artifact(*_args, **_kwargs):
        yield resolved

    reader = SimpleNamespace(
        read_manifest_with_bytes=lambda _uri: (manifest, b"canonical-manifest"),
        open_artifact=open_artifact,
    )

    payload = build_v2_completed_payload(
        context(tmp_path),
        result(),
        duration_seconds=12.5,
        reader_factory=lambda: reader,
    )

    assert payload["result_summary"] == {
        "primary_metric": {"name": "auc", "value": 0.91},
        "sample_rows": {
            "total": 100,
            "train": 70,
            "validation": 10,
            "test": 20,
        },
    }
    training = payload["training_result"]
    assert training["model_features"][0]["feature_id"] == "feature-1"
    assert (
        training["feature_analysis"]["importance_ranking"][0]["importance_score"]
        == 0.75
    )
    manifest_payload = payload["artifact_manifest"]
    file_payload = manifest_payload["model_artifacts"]["model_weights"]["alternatives"][
        0
    ]["files"][0]
    assert file_payload["hash"] == f"sha256:{digest}"
    assert Path(file_payload["uri"]).read_bytes() == b"verified-model"
    assert (
        manifest_payload["manifest_sha256"]
        == hashlib.sha256(b"canonical-manifest").hexdigest()
    )
    json.dumps(payload, allow_nan=False)


@pytest.mark.parametrize("mutation", ["missing", "tampered"])
def test_completed_terminal_fails_if_bundle_artifact_cannot_be_verified(
    tmp_path: Path, mutation: str
) -> None:
    bundle = tmp_path / "bundle-id"
    artifact_path = bundle / "artifacts" / "onnx-model" / "model.onnx"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b"original")
    artifact_file = SimpleNamespace(
        relative_path="model.onnx",
        sha256=hashlib.sha256(b"original").hexdigest(),
        size_bytes=len(b"original"),
        role="model",
    )
    artifact = SimpleNamespace(
        name="onnx-model",
        format="onnx",
        artifact_kind="model",
        tree_digest="a" * 64,
        files=(artifact_file,),
    )
    manifest = SimpleNamespace(
        canonical_uri=str(bundle),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        bundle_id="bundle-id",
        roles={"primary_model": "onnx-model"},
        artifacts=(artifact,),
    )
    if mutation == "missing":
        artifact_path.unlink()
    else:
        artifact_path.write_bytes(b"tampered")
    resolved = SimpleNamespace(
        path_for=lambda relative_path: artifact_path.parent / relative_path
    )

    @contextmanager
    def open_artifact(*_args, **_kwargs):
        yield resolved

    reader = SimpleNamespace(
        read_manifest_with_bytes=lambda _uri: (manifest, b"canonical-manifest"),
        open_artifact=open_artifact,
    )

    with pytest.raises(ValueError, match="missing|integrity mismatch"):
        build_v2_completed_payload(
            context(tmp_path),
            result(),
            duration_seconds=1.0,
            reader_factory=lambda: reader,
        )


def test_requested_metric_missing_fails_instead_of_fabricating_result(
    tmp_path: Path,
) -> None:
    value = result()
    assert isinstance(value["metrics"], dict)
    value["metrics"].pop("eval_auc")
    with pytest.raises(ValueError, match="primary evaluation metric"):
        build_v2_completed_payload(
            context(tmp_path),
            value,
            duration_seconds=1.0,
            reader_factory=lambda: pytest.fail("manifest must not be read"),
        )


@pytest.mark.parametrize(
    ("name", "metrics", "expected"),
    [
        ("rmse", {"eval_rmse": 0.25}, 0.25),
        ("mae", {"eval_mae": 0.2}, 0.2),
        ("r2", {"eval_r2": 0.9}, 0.9),
        ("f1", {"eval_f1_macro": 0.75}, 0.75),
        ("precision", {"eval_precision_macro": 0.8}, 0.8),
        ("recall", {"eval_recall_macro": 0.7}, 0.7),
    ],
)
def test_completion_resolves_regression_and_multiclass_metric_keys(
    name: str, metrics: dict[str, float], expected: float
) -> None:
    assert _metric(metrics, name) == expected


def test_required_row_count_missing_fails_instead_of_defaulting_to_zero(
    tmp_path: Path,
) -> None:
    value = result()
    assert isinstance(value["metrics"], dict)
    value["metrics"].pop("row_count_train")
    with pytest.raises(ValueError, match="missing required row_count_train"):
        build_v2_completed_payload(
            context(tmp_path),
            value,
            duration_seconds=1.0,
            reader_factory=lambda: pytest.fail("manifest must not be read"),
        )


def test_v2_reporter_emits_flat_canonical_terminal(fake_redis: FakeRedis) -> None:
    reporter = RedisEventReporter(
        fake_redis,
        event_stream_prefix="events:training",
        operation_id="job-1",
        operation_type="training",
        execution_profile="distributed",
        run_id="job-1",
        attempt_id="attempt-1",
        wire_protocol_profile="knova-training-v2",
    )
    reporter.publish(
        "COMPLETED",
        {
            "phase": "COMPLETED",
            "duration_seconds": 1.0,
            "result_summary": {},
            "training_result": {},
            "artifact_manifest": {},
        },
        phase="COMPLETED",
    )

    event = json.loads(fake_redis.events["events:training:job-1"][0]["payload"])
    assert event["protocol_version"] == "2.0"
    assert event["job_id"] == "job-1"
    assert event["event_type"] == "COMPLETED"
    assert "payload" not in event
