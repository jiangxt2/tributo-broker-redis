"""KnoVa training control-plane protocol models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

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


class TrainingJobRequest(BaseModel):
    """Minimal envelope needed by the provider before Core training mapping."""

    model_config = ConfigDict(extra="allow")

    protocol_version: str = PROTOCOL_VERSION
    job_id: str = Field(..., min_length=1)
    training_config: dict[str, Any] | None = None

    def require_training_config(self) -> dict[str, Any]:
        """Return the training config or raise a permanent validation error."""
        if not isinstance(self.training_config, dict) or not self.training_config:
            raise ValueError("training_config must be a non-empty object")
        return dict(self.training_config)


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
