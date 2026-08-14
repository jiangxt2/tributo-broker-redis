"""Worker-side Redis cancellation checker."""

from __future__ import annotations

import logging
from typing import Any

from tributo.integrations.broker import CancellationChecker, CancellationSpec

from tributo_broker_redis.config import RedisBrokerConfig
from tributo_broker_redis.redis_client import create_redis_client

logger = logging.getLogger(__name__)


class RedisCancellationChecker(CancellationChecker):
    """Poll a Redis cancel key and continue training when Redis is unavailable."""

    def __init__(
        self, redis_client: Any, config: RedisBrokerConfig, job_id: str
    ) -> None:
        self._redis = redis_client
        self._config = config
        self._job_id = job_id

    def is_cancelled(self, job_id: str) -> bool:
        """Check the cancel key for the job bound when this checker was built.

        The argument is retained because it is part of the Core checker
        contract. A non-empty different identity is rejected rather than
        allowing one worker checker to observe another job's cancellation.
        """
        if job_id and job_id != self._job_id:
            logger.warning(
                "Ignoring cancellation check for unexpected job_id: bound=%s "
                "requested=%s",
                self._job_id,
                job_id,
            )
            return False
        key = self._config.cancel_key(self._job_id)
        try:
            return bool(self._redis.exists(key))
        except Exception:
            logger.warning(
                "Redis cancellation check unavailable; continuing training: job_id=%s",
                job_id or self._job_id,
                exc_info=True,
            )
            return False


def build_cancellation_checker(
    spec: CancellationSpec,
    config: RedisBrokerConfig,
) -> RedisCancellationChecker:
    """Factory used by Core's worker reconstruction path."""
    return RedisCancellationChecker(
        create_redis_client(config),
        config,
        spec.job_id,
    )
