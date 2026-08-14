#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

core_root=${TRIBUTO_CORE_ROOT:-}
if [ -z "$core_root" ]; then
  echo "TRIBUTO_CORE_ROOT must point to the compatible Tributo Core worktree" >&2
  exit 2
fi

project_name="tributo-broker-redis-topology-it-$(date +%Y%m%d%H%M%S)-$$"
compose_file="$repo_root/tests/integrations/docker-compose.topologies.yml"
wheel_dir=$(mktemp -d /tmp/tributo-broker-redis-topology-wheel.XXXXXX)
core_wheel_dir=$(mktemp -d /tmp/tributo-core-topology-wheel.XXXXXX)
venv_dir=$(mktemp -d /tmp/tributo-broker-redis-topology-venv.XXXXXX)
project_started=0

cleanup() {
  if [ "$project_started" -eq 1 ]; then
    docker compose --project-name "$project_name" --file "$compose_file" down --volumes --remove-orphans
  fi
  mv "$wheel_dir" "/tmp/tributo-broker-redis-topology-wheel-complete-$$" 2>/dev/null || true
  mv "$core_wheel_dir" "/tmp/tributo-core-topology-wheel-complete-$$" 2>/dev/null || true
  mv "$venv_dir" "/tmp/tributo-broker-redis-topology-venv-complete-$$" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

uv build --out-dir "$wheel_dir" "$repo_root"
uv build --out-dir "$core_wheel_dir" "$core_root"
provider_wheel=$(find "$wheel_dir" -maxdepth 1 -name 'tributo_broker_redis-*.whl' -print -quit)
core_wheel=$(find "$core_wheel_dir" -maxdepth 1 -name 'tributo-*.whl' -print -quit)
test -n "$provider_wheel"
test -n "$core_wheel"

uv venv "$venv_dir"
python_bin="$venv_dir/bin/python"
uv pip install --python "$python_bin" "$core_wheel" "$provider_wheel" pytest

announce_output=$(docker run --rm --add-host host.docker.internal:host-gateway \
  redis:7-alpine getent ahostsv4 host.docker.internal)
announce_ip=$(printf '%s\n' "$announce_output" | sed -n '1s/[[:space:]].*$//p')
test -n "$announce_ip"
export BROKER_CLUSTER_ANNOUNCE_IP="$announce_ip"
project_started=1
docker compose --project-name "$project_name" --file "$compose_file" up --detach --wait --wait-timeout 60
for _attempt in $(seq 1 60); do
  if docker compose --project-name "$project_name" --file "$compose_file" exec -T cluster-1 redis-cli -p 7000 ping >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker compose --project-name "$project_name" --file "$compose_file" exec -T cluster-1 \
  redis-cli --cluster create \
  host.docker.internal:17000 host.docker.internal:17001 host.docker.internal:17002 \
  --cluster-replicas 0 --cluster-yes

export BROKER_TOPOLOGY_PROJECT="$project_name"
export BROKER_TOPOLOGY_COMPOSE_FILE="$compose_file"
export PYTHONPYCACHEPREFIX=/tmp/tributo-broker-redis-topology-pyc
export BROKER_TOPOLOGY_TEST_LOG="$repo_root/tests/integration/.runtime/topology.log"
mkdir -p "$(dirname "$BROKER_TOPOLOGY_TEST_LOG")"
test_status=0
"$python_bin" -m pytest -p no:cacheprovider -q \
  tests/integration/test_broker_redis_topologies.py -m integration \
  >"$BROKER_TOPOLOGY_TEST_LOG" 2>&1 || test_status=$?
tee /dev/stderr <"$BROKER_TOPOLOGY_TEST_LOG"
exit "$test_status"
