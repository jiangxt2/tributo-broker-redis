"""Lazy Redis client construction for the provider."""

from __future__ import annotations

from typing import Any

import redis
from redis.cluster import ClusterNode

from tributo_broker_redis.config import RedisBrokerConfig


class _AddressRemappingSentinel(redis.Sentinel):
    """Apply provider-configured NAT mappings to Sentinel master addresses."""

    def __init__(
        self,
        *args: Any,
        address_map: dict[str, tuple[str, int]],
        **kwargs: Any,
    ) -> None:
        self._address_map = address_map
        super().__init__(*args, **kwargs)

    def discover_master(self, service_name: str) -> tuple[str, int]:
        address = super().discover_master(service_name)
        return self._address_map.get(f"{address[0]}:{address[1]}", address)


def create_redis_client(config: RedisBrokerConfig) -> Any:
    """Create a redis-py client only when a runtime is explicitly selected."""
    common: dict[str, Any] = {
        "username": config.username,
        "password": config.password(),
        "decode_responses": True,
    }
    # Do not pass ``ssl=False`` to redis-py.  In URL mode it can be forwarded
    # as a connection keyword that older/newer redis-py combinations reject.
    if config.ssl:
        common["ssl"] = True
    common = {key: value for key, value in common.items() if value is not None}
    if config.mode == "standalone":
        if config.url:
            return redis.Redis.from_url(config.url, **common)
        return redis.Redis(
            host=config.host,
            port=config.port,
            db=config.db,
            **common,
        )
    if config.mode == "sentinel":
        sentinel_kwargs: dict[str, Any] = {}
        if config.sentinel_force_master_ip:
            sentinel_kwargs["force_master_ip"] = config.sentinel_force_master_ip
        sentinel_cls: type[redis.Sentinel] = redis.Sentinel
        if config.sentinel_address_map:
            sentinel_cls = _AddressRemappingSentinel
            sentinel_kwargs["address_map"] = config.sentinel_address_map
        sentinel = sentinel_cls(
            config.sentinel_hosts,
            **sentinel_kwargs,
            **common,
        )
        return sentinel.master_for(config.sentinel_service, db=config.db)
    cluster_kwargs: dict[str, Any] = {}
    if config.cluster_address_remap_host:
        cluster_kwargs["address_remap"] = lambda address: (
            config.cluster_address_remap_host,
            address[1],
        )
    return redis.RedisCluster(
        startup_nodes=[
            ClusterNode(host, port) for host, port in config.cluster_startup_nodes
        ],
        **cluster_kwargs,
        **common,
    )
