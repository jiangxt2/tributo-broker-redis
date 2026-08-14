"""Standalone provider diagnostics entry point."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from tributo_broker_redis.plugin import RedisBrokerPlugin


def main() -> int:
    parser = argparse.ArgumentParser(description="Tributo Redis broker provider")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-connectivity", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    RedisBrokerPlugin().validate_config(
        config,
        check_connectivity=args.check_connectivity,
    )
    print("Redis broker configuration is valid")
    return 0
