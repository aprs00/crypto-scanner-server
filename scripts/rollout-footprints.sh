#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
ENV_FILE="$REPO_DIR/.env"
STATE_DIR=/tmp/footprint-state
LOCK_FILE=/tmp/crypto-scanner-footprint-rollout.lock
READY_TIMEOUT_SECONDS=180
HANDOFF_TIMEOUT_SECONDS=120
POLL_SECONDS=2
MINIMUM_COMPOSE_VERSION=2.24.4

COMPOSE=(
  docker compose
  -f "$REPO_DIR/docker-compose.yml"
  -f "$REPO_DIR/docker-compose.prod.yml"
)

TARGET_EXCHANGE=all
TARGET_IMAGE=
IMAGE_WAS_EXPLICIT=false
BOOTSTRAP_LEGACY=false
CANDIDATE=
CUTOVER_STARTED=false

usage() {
  cat <<'EOF'
Usage: scripts/rollout-footprints.sh [OPTIONS]

Options:
  --image IMAGE          Roll out this already-built image. Defaults to
                         APP_IMAGE in .env.
  --exchange EXCHANGE    all, binance, bybit, or hyperliquid (default: all).
  --bootstrap-legacy     Allow the old collector to lack rollout state files.
                         Use only for the first lock-aware rollout.
  -h, --help             Show this help.
EOF
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  return 1
}

cleanup_on_exit() {
  local status=$?
  if [[ -n "$CANDIDATE" ]] && docker inspect "$CANDIDATE" >/dev/null 2>&1; then
    if [[ "$CUTOVER_STARTED" == false ]]; then
      printf 'Cleaning up pre-cutover candidate %s\n' "$CANDIDATE" >&2
      docker stop --time 60 "$CANDIDATE" >/dev/null 2>&1 || true
      docker rm "$CANDIDATE" >/dev/null 2>&1 || true
    elif (( status != 0 )); then
      printf '%s\n' \
        "Cutover had started; leaving $CANDIDATE unchanged for safe recovery." \
        "Inspect it with: docker logs --tail 100 $CANDIDATE" >&2
    fi
  fi
  exit "$status"
}
trap cleanup_on_exit EXIT

while (( $# )); do
  case "$1" in
    --image)
      (( $# >= 2 )) || fail "--image requires a value"
      TARGET_IMAGE=$2
      IMAGE_WAS_EXPLICIT=true
      shift 2
      ;;
    --image=*)
      TARGET_IMAGE=${1#*=}
      IMAGE_WAS_EXPLICIT=true
      shift
      ;;
    --exchange)
      (( $# >= 2 )) || fail "--exchange requires a value"
      TARGET_EXCHANGE=$2
      shift 2
      ;;
    --exchange=*)
      TARGET_EXCHANGE=${1#*=}
      shift
      ;;
    --bootstrap-legacy)
      BOOTSTRAP_LEGACY=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

case "$TARGET_EXCHANGE" in
  all|binance|bybit|hyperliquid) ;;
  *) fail "unsupported exchange: $TARGET_EXCHANGE" ;;
esac

read_image_from_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    fail "$ENV_FILE does not exist"
    return
  fi
  local value
  value=$(sed -n \
    's/^[[:space:]]*APP_IMAGE[[:space:]]*=[[:space:]]*//p' \
    "$ENV_FILE" | tail -n 1 | tr -d '\r')
  if [[ "$value" == \"*\" && "$value" == *\" ]]; then
    value=${value:1:${#value}-2}
  elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
    value=${value:1:${#value}-2}
  fi
  if [[ -z "$value" ]]; then
    fail "APP_IMAGE is not set in $ENV_FILE; pass --image"
    return
  fi
  printf '%s\n' "$value"
}

if [[ -z "$TARGET_IMAGE" ]]; then
  TARGET_IMAGE=$(read_image_from_env)
fi
[[ -n "$TARGET_IMAGE" ]] || fail "image cannot be empty"

command -v docker >/dev/null || fail "docker is not installed"
command -v flock >/dev/null || fail "flock is not installed"
compose_version=$(docker compose version --short 2>/dev/null | sed 's/^v//') ||
  fail "Docker Compose v2 is unavailable"
if [[ $(printf '%s\n%s\n' "$MINIMUM_COMPOSE_VERSION" "$compose_version" |
  sort -V | head -n 1) != "$MINIMUM_COMPOSE_VERSION" ]]; then
  fail "Docker Compose $MINIMUM_COMPOSE_VERSION or newer is required (found $compose_version)"
fi
docker image inspect "$TARGET_IMAGE" >/dev/null 2>&1 ||
  fail "image is not present locally: $TARGET_IMAGE"

exec 9>"$LOCK_FILE"
flock -n 9 || fail "another footprint rollout is already running"

container_running() {
  [[ $(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null) == true ]]
}

state_exists() {
  docker exec "$1" test -e "$STATE_DIR/$2" >/dev/null 2>&1
}

show_failure_context() {
  local container=$1
  printf '%s\n' "Recent logs for $container:" >&2
  docker logs --tail 80 "$container" >&2 || true
}

wait_for_states() {
  local container=$1
  local timeout=$2
  shift 2
  local deadline=$(( $(date +%s) + timeout ))
  local state

  while (( $(date +%s) < deadline )); do
    if ! container_running "$container"; then
      show_failure_context "$container"
      fail "$container exited while waiting for state: $*"
      return
    fi
    local ready=true
    for state in "$@"; do
      if ! state_exists "$container" "$state"; then
        ready=false
        break
      fi
    done
    if [[ "$ready" == true ]]; then
      return 0
    fi
    sleep "$POLL_SECONDS"
  done

  show_failure_context "$container"
  fail "timed out waiting for $container state: $*"
}

stop_cleanly() {
  local container=$1
  if ! docker stop --time 60 "$container" >/dev/null; then
    fail "could not stop $container"
    return
  fi
  if container_running "$container"; then
    fail "$container is still running after its stop timeout"
    return
  fi
  local exit_code
  if ! exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$container"); then
    fail "could not inspect the exit status of $container"
    return
  fi
  [[ "$exit_code" == 0 ]] || fail "$container did not exit cleanly (code $exit_code)"
}

require_verified_handoff() {
  local container=$1
  state_exists "$container" handoff-verified ||
    fail "$container became active without verified feed overlap; collection continues, but a gap is possible"
}

promote() {
  local container=$1
  docker kill --signal USR1 "$container" >/dev/null
  wait_for_states "$container" "$HANDOFF_TIMEOUT_SECONDS" writer-active
  require_verified_handoff "$container"
}

rollout_exchange() {
  local exchange=$1
  local service="${exchange}-footprints"
  local canonical="cs-${exchange}-footprints"
  local candidate="${canonical}-candidate"
  CANDIDATE=
  CUTOVER_STARTED=false

  printf 'Rolling %s footprints to %s\n' "$exchange" "$TARGET_IMAGE"

  docker inspect "$canonical" >/dev/null 2>&1 ||
    fail "canonical collector does not exist: $canonical"
  container_running "$canonical" || fail "canonical collector is not running: $canonical"
  if docker inspect "$candidate" >/dev/null 2>&1; then
    fail "candidate already exists; inspect or remove it first: $candidate"
  fi
  if [[ "$BOOTSTRAP_LEGACY" == false ]]; then
    state_exists "$canonical" writer-active || fail "$canonical is not writer-active"
  fi

  CANDIDATE=$candidate
  APP_IMAGE="$TARGET_IMAGE" "${COMPOSE[@]}" run -d --no-deps \
    --name "$CANDIDATE" "$service" \
    python manage.py footprint_trades --exchange "$exchange" --standby >/dev/null
  docker update --restart=no "$CANDIDATE" >/dev/null
  wait_for_states "$CANDIDATE" "$READY_TIMEOUT_SECONDS" feeds-ready

  CUTOVER_STARTED=true
  if ! stop_cleanly "$canonical"; then
    if [[ "$BOOTSTRAP_LEGACY" == true ]] && container_running "$canonical"; then
      CUTOVER_STARTED=false
      printf '%s\n' \
        "$canonical is still running without writer locking; keeping the candidate unpromoted." >&2
    else
      printf '%s\n' \
        "$canonical stopped uncleanly; promoting the lock-aware warm candidate before aborting." >&2
      promote "$CANDIDATE" || true
    fi
    fail "$canonical must exit cleanly before its replacement is created"
  fi

  promote "$CANDIDATE"

  APP_IMAGE="$TARGET_IMAGE" "${COMPOSE[@]}" up -d --no-deps \
    --force-recreate "$service" >/dev/null
  wait_for_states "$canonical" "$READY_TIMEOUT_SECONDS" feeds-ready

  if ! stop_cleanly "$CANDIDATE"; then
    printf '%s\n' \
      "$CANDIDATE stopped uncleanly; waiting for the canonical collector before aborting." >&2
    if wait_for_states "$canonical" "$HANDOFF_TIMEOUT_SECONDS" \
      writer-active; then
      require_verified_handoff "$canonical" || true
    fi
    fail "$CANDIDATE did not exit cleanly"
  fi
  wait_for_states "$canonical" "$HANDOFF_TIMEOUT_SECONDS" writer-active
  require_verified_handoff "$canonical"

  docker rm "$CANDIDATE" >/dev/null
  CANDIDATE=
  CUTOVER_STARTED=false
  printf 'Completed %s footprint rollout\n' "$exchange"
}

persist_image() {
  local temporary
  temporary=$(mktemp "${ENV_FILE}.tmp.XXXXXX")
  if ! awk -v image="$TARGET_IMAGE" '
    BEGIN { replaced = 0 }
    /^[[:space:]]*APP_IMAGE[[:space:]]*=/ {
      if (!replaced) {
        print "APP_IMAGE=" image
        replaced = 1
      }
      next
    }
    { print }
    END {
      if (!replaced) print "APP_IMAGE=" image
    }
  ' "$ENV_FILE" >"$temporary"; then
    rm -f "$temporary"
    fail "collectors use $TARGET_IMAGE, but APP_IMAGE could not be updated in $ENV_FILE"
  fi
  chmod --reference="$ENV_FILE" "$temporary"
  mv -f "$temporary" "$ENV_FILE"
  printf 'Persisted APP_IMAGE=%s in %s\n' "$TARGET_IMAGE" "$ENV_FILE"
}

cd "$REPO_DIR"
if [[ "$TARGET_EXCHANGE" == all ]]; then
  for exchange in binance bybit hyperliquid; do
    rollout_exchange "$exchange"
  done
else
  rollout_exchange "$TARGET_EXCHANGE"
fi

if [[ "$TARGET_EXCHANGE" == all && "$IMAGE_WAS_EXPLICIT" == true ]]; then
  persist_image
fi

printf 'Footprint rollout finished successfully.\n'
