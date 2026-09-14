#!/usr/bin/env bash
# One handoff: the warmed candidate becomes the permanent collector.
set -Eeuo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
image=${1:?Usage: scripts/rollout-footprints.sh IMAGE [all|binance|bybit|hyperliquid]}
exchange=${2:-all}
[[ $exchange =~ ^(all|binance|bybit|hyperliquid)$ ]] || exit 2
[[ $image =~ ^[a-zA-Z0-9][a-zA-Z0-9._:/@-]+$ ]] || exit 2
exec 9>/tmp/crypto-scanner-footprint-rollout.lock
flock -n 9 || { printf 'Another rollout is running\n' >&2; exit 1; }
compose=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)
candidate=
cutover=false
cleanup() {
    if [[ -n $candidate && $cutover == false ]]; then
        docker stop --time 45 "$candidate" >/dev/null 2>&1 || true
        docker rm "$candidate" >/dev/null 2>&1 || true
    fi
    if [[ -n $candidate && $cutover == true ]]; then
        printf 'Cutover started; candidate retained: %s. Inspect its health before intervening.\n' "$candidate" >&2
    fi
}
trap cleanup EXIT
wait_ready() {
    local container=$1
    shift
    local deadline=$((SECONDS+180))
    while (( SECONDS < deadline )); do
        if docker exec "$container" footprints health --overlap "$@" >/dev/null 2>&1; then return; fi
        sleep 1
    done
    printf 'Timed out waiting for verified overlap: %s\n' "$container" >&2
    return 1
}
roll() {
    local venue=$1 old="cs-$1-footprints" instance="rollout-$1-$(date +%s)-$$"
    docker exec "$old" footprints health --ready >/dev/null
    candidate="$old-$instance"
    BINANCE_FOOTPRINT_IMAGE="$image" BYBIT_FOOTPRINT_IMAGE="$image" HYPERLIQUID_FOOTPRINT_IMAGE="$image" \
        "${compose[@]}" run -d --no-deps --use-aliases --name "$candidate" \
        -e "FOOTPRINT_INSTANCE=$instance" "$venue-footprints" collect --exchange "$venue" --standby >/dev/null
    docker update --restart=no "$candidate" >/dev/null
    wait_ready "$candidate"
    cutover=true
    docker update --restart=no "$old" >/dev/null
    # The old process drains voluntarily; no timeout force-kill is used.
    docker exec "$old" footprints control drain
    local deadline=$((SECONDS+50))
    while [[ $(docker inspect --format '{{.State.Running}}' "$old") == true ]]; do
        (( SECONDS < deadline )) || { printf 'Old writer did not drain: %s\n' "$old" >&2; return 1; }
        sleep 1
    done
    docker exec "$candidate" footprints control promote
    wait_ready "$candidate" --writer
    docker update --restart=unless-stopped "$candidate" >/dev/null
    docker rm "$old" >/dev/null
    docker rename "$candidate" "$old"
    candidate=
    cutover=false
    local pin="${venue^^}_FOOTPRINT_IMAGE" temporary
    temporary=$(mktemp .env.XXXXXX)
    awk -v key="$pin" -v image="$image" 'BEGIN{done=0} $0 ~ "^" key "=" {if(!done){print key "=" image;done=1};next} {print} END{if(!done)print key "=" image}' .env > "$temporary"
    chmod --reference=.env "$temporary"
    mv "$temporary" .env
    printf '%s collector now runs %s\n' "$venue" "$image"
}
if [[ $exchange == all ]]; then
    for venue in binance bybit hyperliquid; do roll "$venue"; done
else
    roll "$exchange"
fi
