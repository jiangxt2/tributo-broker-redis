"""Topology-aware, credential-safe redis-py client construction."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import redis
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import (
    AskError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    CrossSlotTransactionError,
    MasterDownError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
)
from redis.sentinel import MasterNotFoundError, Sentinel

from tributo_broker_redis.config import RedisBrokerConfig, RedisTransportConfig

_TRANSIENT = (
    ConnectionError,
    TimeoutError,
    ClusterDownError,
    MasterDownError,
    MasterNotFoundError,
    TryAgainError,
    AskError,
    ClusterError,
    SlotNotCoveredError,
)


def is_transient_redis_error(error: BaseException) -> bool:
    if isinstance(error, (ClusterCrossSlotError, CrossSlotTransactionError)):
        return False
    return isinstance(error, _TRANSIENT)


def _secret(name: str | None) -> str | None:
    if name is None:
        return None
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Redis credential environment {name!r} is unavailable")
    return value


def _tls(config: RedisTransportConfig) -> dict[str, Any]:
    paths = {
        "ssl_ca_certs": config.tls_ca_cert_path,
        "ssl_certfile": config.tls_cert_path,
        "ssl_keyfile": config.tls_key_path,
    }
    for value in paths.values():
        if value is not None and not Path(value).is_file():
            raise RuntimeError("Redis TLS mount is unavailable")
    result = {name: value for name, value in paths.items() if value is not None}
    if all(urlsplit(value).scheme == "rediss" for value in config.connection_urls):
        result["ssl_cert_reqs"] = "required"
    return result


def _endpoint(url: str) -> tuple[str, int, bool]:
    parsed = urlsplit(url)
    assert parsed.hostname is not None
    return parsed.hostname, parsed.port or 6379, parsed.scheme == "rediss"


def _common(config: RedisTransportConfig) -> dict[str, Any]:
    return {
        "decode_responses": True,
        "socket_timeout": config.socket_timeout_seconds,
        "username": _secret(config.username_env),
        "password": _secret(config.password_env),
        **_tls(config),
    }


def create_redis_client(
    config: RedisBrokerConfig | RedisTransportConfig | Mapping[str, Any] | str,
) -> Any:
    """Rebuild the same standalone/Sentinel/Cluster client in every process."""
    if isinstance(config, RedisBrokerConfig):
        transport = config.transport
    elif isinstance(config, RedisTransportConfig):
        transport = config
    elif isinstance(config, str):
        transport = RedisTransportConfig(url=config)
    else:
        transport = RedisTransportConfig.model_validate(config)
    common = _common(transport)
    if transport.mode == "standalone":
        return redis.Redis.from_url(transport.url, db=transport.database, **common)
    if transport.mode == "sentinel":
        endpoints = [_endpoint(value) for value in transport.sentinel_urls]
        ssl = all(item[2] for item in endpoints)
        sentinel = Sentinel(
            [(host, port) for host, port, _ssl in endpoints],
            socket_timeout=transport.socket_timeout_seconds,
            sentinel_kwargs={
                "username": _secret(transport.sentinel_username_env),
                "password": _secret(transport.sentinel_password_env),
                "ssl": ssl,
                **_tls(transport),
            },
        )
        return sentinel.master_for(
            transport.sentinel_master_name,
            db=transport.database,
            ssl=ssl,
            **common,
        )
    endpoints = [_endpoint(value) for value in transport.cluster_urls]
    ssl = all(item[2] for item in endpoints)
    return RedisCluster(
        startup_nodes=[ClusterNode(host, port) for host, port, _ssl in endpoints],
        ssl=ssl,
        **common,
    )


__all__ = ["create_redis_client", "is_transient_redis_error"]
