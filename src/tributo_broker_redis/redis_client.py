"""Lazy standalone Redis client construction."""

from __future__ import annotations

from typing import Any

import redis

from tributo_broker_redis.config import RedisBrokerConfig, RedisTransportConfig


def create_redis_client(
    config: RedisBrokerConfig | RedisTransportConfig | str,
) -> Any:
    """Create a decoded redis-py client without resolving any credential."""
    if isinstance(config, RedisBrokerConfig):
        url = config.transport.url
    elif isinstance(config, RedisTransportConfig):
        url = config.url
    else:
        url = config
    return redis.Redis.from_url(url, decode_responses=True)


__all__ = ["create_redis_client"]
