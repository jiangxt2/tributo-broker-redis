"""Wheel metadata contract for Tributo Broker discovery."""

from __future__ import annotations

import tomllib
from pathlib import Path


def test_tributo_broker_entrypoint_is_declared() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    with pyproject.open("rb") as stream:
        project = tomllib.load(stream)["project"]
    entrypoints = project["entry-points"]["tributo.brokers"]
    assert entrypoints["knova-redis"] == (
        "tributo_broker_redis.plugin:RedisBrokerPlugin"
    )
