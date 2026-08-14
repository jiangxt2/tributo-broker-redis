"""Wheel-only entry-point discovery validation."""

from __future__ import annotations

from importlib.metadata import entry_points

import pytest

pytestmark = pytest.mark.integration


def test_installed_provider_is_discoverable_through_tributo_entry_points() -> None:
    matches = [
        entry
        for entry in entry_points(group="tributo.brokers")
        if entry.name == "knova-redis"
    ]
    assert len(matches) == 1
    plugin_class = matches[0].load()
    assert plugin_class.broker_id == "knova-redis"
