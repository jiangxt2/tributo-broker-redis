#!/usr/bin/env sh
set -eu

if rg -n --glob '*.py' \
  '#\s*(type:\s*ignore|mypy:\s*ignore|pyright:\s*ignore)' \
  src tests; then
  echo "Provider source/tests must not add type-checker suppressions" >&2
  exit 1
fi
