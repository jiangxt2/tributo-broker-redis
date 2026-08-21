"""Public configuration for the Redis Streams provider."""

from __future__ import annotations

import os
import re
import socket
import uuid
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any, Literal
from urllib.parse import urlsplit

from packaging.requirements import InvalidRequirement, Requirement
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from tributo_broker_redis.protocol_v2 import MIN_TERMINAL_EVENT_BYTES

OperationType = Literal["training", "batch_inference"]
ExecutionProfile = Literal["single_worker", "distributed"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class RedisTransportConfig(_StrictModel):
    """Credential-free Redis topology used by every provider process."""

    mode: Literal["standalone", "sentinel", "cluster"] = "standalone"
    url: str = "redis://127.0.0.1:6379/0"
    driver_url: str | None = None
    sentinel_urls: tuple[str, ...] = ()
    sentinel_master_name: str | None = Field(default=None, min_length=1)
    cluster_urls: tuple[str, ...] = ()
    username_env: str | None = None
    password_env: str | None = None
    sentinel_username_env: str | None = None
    sentinel_password_env: str | None = None
    database: int = Field(default=0, ge=0, le=15)
    tls_ca_cert_path: str | None = None
    tls_cert_path: str | None = None
    tls_key_path: str | None = None
    socket_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    block_ms: int = Field(default=1000, ge=1, le=60000)
    claim_idle_ms: int = Field(default=60000, ge=0)
    claim_count: int = Field(default=10, ge=1, le=1000)
    max_payload_bytes: int = Field(default=1024 * 1024, ge=1)
    max_event_bytes: int = Field(default=1024 * 1024, ge=MIN_TERMINAL_EVENT_BYTES)
    max_stream_length: int = Field(default=1000, ge=1)

    @field_validator("url", "driver_url")
    @classmethod
    def _credential_free_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
            raise ValueError("Redis URL must use redis:// or rediss:// with a host")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Redis URL must not contain credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("Redis URL must not contain query or fragment")
        return value

    @field_validator("sentinel_urls", "cluster_urls")
    @classmethod
    def _credential_free_nodes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(values)) != len(values):
            raise ValueError("Redis topology URLs must be unique")
        for value in values:
            cls._credential_free_url(value)
        return values

    @field_validator(
        "username_env",
        "password_env",
        "sentinel_username_env",
        "sentinel_password_env",
    )
    @classmethod
    def _credential_environment_name(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("Redis credential env reference is invalid")
        return value

    @field_validator("tls_ca_cert_path", "tls_cert_path", "tls_key_path")
    @classmethod
    def _absolute_tls_path(cls, value: str | None) -> str | None:
        if value is not None and (not value.startswith("/") or "\x00" in value):
            raise ValueError("Redis TLS files must use absolute mount paths")
        return value

    @model_validator(mode="after")
    def _topology_contract(self) -> RedisTransportConfig:
        if self.mode == "standalone":
            if (
                self.sentinel_urls
                or self.sentinel_master_name
                or self.sentinel_username_env
                or self.sentinel_password_env
                or self.cluster_urls
            ):
                raise ValueError("standalone mode rejects Sentinel/Cluster fields")
            for value in self.connection_urls:
                path = urlsplit(value).path
                url_database = int(path.lstrip("/") or 0)
                if url_database != self.database:
                    raise ValueError("standalone URL database path must match database")
        elif self.mode == "sentinel":
            if not self.sentinel_urls or not self.sentinel_master_name:
                raise ValueError(
                    "sentinel mode requires sentinel_urls and sentinel_master_name"
                )
            if self.cluster_urls or self.driver_url is not None:
                raise ValueError("sentinel mode rejects Cluster/driver_url fields")
            if any(
                urlsplit(value).path not in {"", "/"} for value in self.sentinel_urls
            ):
                raise ValueError("Sentinel node URLs must not contain a database path")
        else:
            if not self.cluster_urls:
                raise ValueError("cluster mode requires cluster_urls")
            if (
                self.sentinel_urls
                or self.sentinel_master_name
                or self.sentinel_username_env
                or self.sentinel_password_env
                or self.driver_url
            ):
                raise ValueError("cluster mode rejects Sentinel/driver_url fields")
            for value in self.cluster_urls:
                path = urlsplit(value).path
                if path not in {"", "/", "/0"}:
                    raise ValueError("Redis Cluster only supports database 0")
            if self.database != 0:
                raise ValueError("Redis Cluster only supports database 0")
        tls_values = (
            self.tls_ca_cert_path,
            self.tls_cert_path,
            self.tls_key_path,
        )
        if len({urlsplit(value).scheme for value in self.connection_urls}) != 1:
            raise ValueError("Redis topology URLs must use one TLS mode")
        if any(tls_values) and not all(
            urlsplit(value).scheme == "rediss" for value in self.connection_urls
        ):
            raise ValueError("Redis TLS files require rediss:// topology URLs")
        if (self.tls_cert_path is None) != (self.tls_key_path is None):
            raise ValueError(
                "Redis client certificate and key must be configured together"
            )
        return self

    @property
    def connection_urls(self) -> tuple[str, ...]:
        if self.mode == "sentinel":
            return self.sentinel_urls
        if self.mode == "cluster":
            return self.cluster_urls
        return (self.url,) if self.driver_url is None else (self.url, self.driver_url)

    @property
    def ray_driver_url(self) -> str:
        return self.driver_url or self.url

    def connection_descriptor(self, *, for_driver: bool = False) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        if for_driver and self.mode == "standalone":
            value["url"] = self.ray_driver_url
            value["driver_url"] = None
        return value


class ChannelConfig(_StrictModel):
    """Independent task, event, cancel, and consumer-group names."""

    task_stream_key: str = Field(min_length=1)
    event_stream_prefix: str = Field(min_length=1)
    cancel_key_prefix: str = Field(min_length=1)
    consumer_group: str = Field(min_length=1)
    consumer_name: str = Field(default_factory=lambda: _default_consumer_name())
    group_start_id: str = Field(default="0-0", min_length=1)
    outer_identity_field: str = "operation_id"

    @field_validator("outer_identity_field")
    @classmethod
    def _valid_outer_identity_field(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}", value):
            raise ValueError("outer_identity_field is not a valid Redis field name")
        if value == "payload":
            raise ValueError("outer_identity_field must not be payload")
        return value

    def event_stream_key(
        self, operation_id: str, *, redis_hash_tag: str | None = None
    ) -> str:
        identity = f"{{{redis_hash_tag}}}" if redis_hash_tag else operation_id
        return f"{self.event_stream_prefix}:{identity}"

    def cancel_key(
        self, operation_id: str, *, redis_hash_tag: str | None = None
    ) -> str:
        identity = f"{{{redis_hash_tag}}}" if redis_hash_tag else operation_id
        return f"{self.cancel_key_prefix}:{identity}"


class ChannelSet(_StrictModel):
    training: ChannelConfig
    batch_inference: ChannelConfig

    def for_operation(self, operation_type: OperationType) -> ChannelConfig:
        return getattr(self, operation_type)

    @model_validator(mode="after")
    def _require_independent_channels(self) -> ChannelSet:
        for attribute in (
            "task_stream_key",
            "event_stream_prefix",
            "cancel_key_prefix",
            "consumer_group",
        ):
            if getattr(self.training, attribute) == getattr(
                self.batch_inference, attribute
            ):
                raise ValueError(
                    f"training and batch_inference require distinct {attribute}"
                )
        return self


class ProtocolProfileConfig(_StrictModel):
    profile: Literal["tributo-generic-v1"] = "tributo-generic-v1"
    protocol_version: Literal["1.0"] = "1.0"


class OperationExecutionConfig(_StrictModel):
    execution_profiles: tuple[ExecutionProfile, ...] = (
        "single_worker",
        "distributed",
    )

    @field_validator("execution_profiles")
    @classmethod
    def _unique_profiles(
        cls, value: tuple[ExecutionProfile, ...]
    ) -> tuple[ExecutionProfile, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("execution_profiles must be non-empty and unique")
        return value


class OperationSet(_StrictModel):
    training: OperationExecutionConfig = Field(default_factory=OperationExecutionConfig)
    batch_inference: OperationExecutionConfig = Field(
        default_factory=OperationExecutionConfig
    )

    def for_operation(self, operation_type: OperationType) -> OperationExecutionConfig:
        return getattr(self, operation_type)


_SENSITIVE_ENV_NAME = re.compile(
    r"(?:password|passwd|secret|token|access[_-]?key|private[_-]?key)", re.IGNORECASE
)
_RESERVED_EXECUTION_ENV_NAMES = frozenset(
    {
        "TRIBUTO_ATTEMPT_ID",
        "TRIBUTO_REDIS_DRIVER_INPUT_B64",
        "TRIBUTO_RUN_ID",
        "TRIBUTO_SUBMISSION_ID",
    }
)
_REMOTE_MODULE_SCHEMES = frozenset({"gs", "http", "https", "s3"})
_REMOTE_MODULE_SUFFIXES = frozenset({".whl", ".zip"})


def _validate_extension_module(value: str) -> str:
    normalized = value.strip()
    if not normalized or "\x00" in normalized:
        raise ValueError("execution.extra_py_modules entries must not be empty")
    if "://" not in normalized:
        if "?" in normalized or "#" in normalized:
            raise ValueError(
                "execution.extra_py_modules local paths must not contain "
                "query or fragment"
            )
        return normalized

    parsed = urlsplit(normalized)
    if parsed.scheme not in _REMOTE_MODULE_SCHEMES or not parsed.hostname:
        raise ValueError("execution.extra_py_modules contains an unsupported URI")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("execution.extra_py_modules must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError(
            "execution.extra_py_modules URI must not contain query or fragment"
        )
    if PurePosixPath(parsed.path).suffix.lower() not in _REMOTE_MODULE_SUFFIXES:
        raise ValueError(
            "execution.extra_py_modules remote URI must reference a wheel or zip"
        )
    return normalized


def _validate_runtime_pip_package(value: str) -> str:
    normalized = value.strip()
    if not normalized or "\x00" in normalized:
        raise ValueError("execution.runtime_pip_packages entries must not be empty")
    if normalized.startswith("-"):
        raise ValueError(
            "execution.runtime_pip_packages does not accept pip command options"
        )
    if "://" in normalized or "@" in normalized:
        raise ValueError("execution.runtime_pip_packages does not accept direct URLs")
    if normalized.lower().endswith(".whl"):
        if any(character.isspace() for character in normalized):
            raise ValueError(
                "execution.runtime_pip_packages wheel paths must not contain whitespace"
            )
        return normalized
    try:
        requirement = Requirement(normalized)
    except InvalidRequirement as exc:
        raise ValueError(
            "execution.runtime_pip_packages requires a PEP 508 requirement "
            "or wheel path"
        ) from exc
    if requirement.url is not None:
        raise ValueError("execution.runtime_pip_packages does not accept direct URLs")
    return normalized


class ExecutionConfig(_StrictModel):
    ray_dashboard_url: str = "http://127.0.0.1:8265"
    project_root: str = Field(min_length=1)
    extra_py_modules: tuple[str, ...] = ()
    runtime_pip_packages: tuple[str, ...] = ()
    env_vars: dict[str, str] = Field(default_factory=dict)
    cancel_poll_interval_seconds: float = Field(default=0.5, gt=0, le=30)
    entrypoint_num_cpus: float = Field(default=1.0, ge=0)

    @field_validator("extra_py_modules")
    @classmethod
    def _trusted_extension_modules(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_validate_extension_module(item) for item in value)

    @field_validator("runtime_pip_packages")
    @classmethod
    def _trusted_runtime_pip_packages(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_validate_runtime_pip_package(item) for item in value)

    @field_validator("env_vars")
    @classmethod
    def _non_sensitive_env(cls, value: dict[str, str]) -> dict[str, str]:
        reserved = sorted(_RESERVED_EXECUTION_ENV_NAMES.intersection(value))
        if reserved:
            raise ValueError(
                "execution.env_vars cannot override reserved runtime identity: "
                + ", ".join(reserved)
            )
        sensitive = sorted(name for name in value if _SENSITIVE_ENV_NAME.search(name))
        if sensitive:
            raise ValueError(
                "execution.env_vars cannot carry sensitive values; offending names: "
                + ", ".join(sensitive)
            )
        return dict(value)

    @model_validator(mode="after")
    def _require_driver_distribution(self) -> ExecutionConfig:
        if not self.extra_py_modules and not self.runtime_pip_packages:
            raise ValueError(
                "execution requires extra_py_modules or runtime_pip_packages "
                "to distribute the provider execution driver"
            )
        return self


class DurabilityConfig(_StrictModel):
    """Restart-safe operation reconciliation and terminal publication."""

    enabled: bool = False
    active_key_prefix: str = Field(default="tributo:active", min_length=1)
    terminal_candidate_key_prefix: str = Field(
        default="tributo:terminal-candidate", min_length=1
    )
    active_ttl_seconds: int = Field(default=7 * 24 * 60 * 60, ge=60)
    terminal_candidate_ttl_seconds: int = Field(default=7 * 24 * 60 * 60, ge=60)
    supervisor_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    supervisor_scan_count: int = Field(default=100, ge=1, le=1000)
    default_timeout_seconds: int | None = Field(default=None, ge=1)

    @field_validator("active_key_prefix", "terminal_candidate_key_prefix")
    @classmethod
    def _safe_key_prefix(cls, value: str) -> str:
        if any(character in value for character in "{}\x00\r\n"):
            raise ValueError(
                "durability key prefixes must not contain braces or controls"
            )
        return value.rstrip(":")

    @staticmethod
    def _identity(operation_type: OperationType, operation_id: str) -> str:
        return f"{{{operation_type}:{operation_id}}}"

    def active_key(self, operation_type: OperationType, operation_id: str) -> str:
        return (
            f"{self.active_key_prefix}:{self._identity(operation_type, operation_id)}"
        )

    def terminal_candidate_key(
        self, operation_type: OperationType, operation_id: str
    ) -> str:
        return (
            f"{self.terminal_candidate_key_prefix}:"
            f"{self._identity(operation_type, operation_id)}"
        )


class RedisBrokerConfig(_StrictModel):
    """Normalized provider root configuration; this is the only accepted shape."""

    broker_id: Literal["tributo-redis"] = "tributo-redis"
    api_version: Literal[1] = 1
    transport: RedisTransportConfig
    channels: ChannelSet
    protocol: ProtocolProfileConfig = Field(default_factory=ProtocolProfileConfig)
    operations: OperationSet = Field(default_factory=OperationSet)
    execution: ExecutionConfig
    durability: DurabilityConfig = Field(default_factory=DurabilityConfig)
    accept_knova_v2: bool = False
    allow_legacy_training_config: bool = False

    @model_validator(mode="after")
    def _v2_requires_durability(self) -> RedisBrokerConfig:
        if self.accept_knova_v2 and not self.durability.enabled:
            raise ValueError("accept_knova_v2 requires durability.enabled=true")
        return self

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> RedisBrokerConfig:
        return normalize_config(value)


def normalize_config(value: Mapping[str, Any]) -> RedisBrokerConfig:
    """Validate one explicit public config shape without compatibility fallbacks."""
    return RedisBrokerConfig.model_validate(dict(value))


def _default_consumer_name() -> str:
    hostname = socket.gethostname().replace(" ", "-")
    return f"tributo-{hostname}-{os.getpid()}-{uuid.uuid4().hex[:10]}"


__all__ = [
    "ChannelConfig",
    "ChannelSet",
    "ExecutionConfig",
    "ExecutionProfile",
    "OperationExecutionConfig",
    "OperationSet",
    "OperationType",
    "ProtocolProfileConfig",
    "RedisBrokerConfig",
    "RedisTransportConfig",
    "normalize_config",
]
