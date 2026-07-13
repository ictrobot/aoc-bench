#!/bin/bash
set -eou pipefail

SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"
SCRIPT_ARGS=("$@")

# Environment-specific paths
REPO_DIR="${AOC_BENCH_DIR:-$HOME/aoc-bench}"
AOC_RS_BENCH_DIR="$REPO_DIR/tools/aoc-rs-bench"
DATA_DIR="$REPO_DIR/data"
INPUTS_DIR="${AOC_INPUTS_DIR:-$HOME/inputs}"
BENCH_BIN="target/release/aoc-bench"
WEB_DIR="$REPO_DIR/web"
WEB_DIST_DIR="$WEB_DIR/dist"
WEB_DATA_DIR="$WEB_DIST_DIR/data"

# Deployment config
STATE_DIR="${AOC_BENCH_STATE_DIR:-$HOME/.local/state/aoc-bench}"
LAST_DEPLOY_EPOCH_FILE="$STATE_DIR/last_deploy_epoch"
LAST_DEPLOY_SNAPSHOT_FILE="$STATE_DIR/last_deployed_snapshot_id"
CLOUDFLARE_ENV_FILE="${CLOUDFLARE_ENV_FILE:-$HOME/.config/aoc-bench/cloudflare.env}"
DEPLOY_INTERVAL_SECONDS=7200

run_on_all_cores() {
    # The builds and config generation aren't ran at the same time as benchmarks, so opt in to running on every core,
    # including the isolated benchmark cores, for speed.
    taskset -c "$(cat /sys/devices/system/cpu/online)" "$@"
}

read_last_deploy_epoch() {
    local value
    value=0
    if [[ -f "$LAST_DEPLOY_EPOCH_FILE" ]] && read -r value < "$LAST_DEPLOY_EPOCH_FILE"; then
        :
    fi
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        value=0
    fi

    printf '%s\n' "$value"
}

maybe_sync_and_deploy() {
    local now last_deploy_epoch age snapshot_id last_snapshot_id
    local command

    if [[ "${AOC_BENCH_ENABLE_WEB_EXPORT:-true}" != "true" ]]; then
        echo "Web export/deploy disabled, skipping deploy attempt" >&2
        return 0
    fi

    if [[ ! -f "$CLOUDFLARE_ENV_FILE" ]]; then
        echo "Cloudflare env file '$CLOUDFLARE_ENV_FILE' not found, skipping deploy attempt" >&2
        return 0
    fi

    mkdir -p "$STATE_DIR"

    now=$(date +%s)
    last_deploy_epoch=$(read_last_deploy_epoch)
    age=$(( now - last_deploy_epoch ))

    if (( last_deploy_epoch > 0 && age < DEPLOY_INTERVAL_SECONDS )); then
        echo "Skipping sync/deploy: last deploy attempt was ${age}s ago (< ${DEPLOY_INTERVAL_SECONDS}s)" >&2
        return 0
    fi

    echo "Attempting sync and deploy..." >&2

    if ! (cd "$WEB_DIR" && npm ci && npm run build); then
        echo "Web build failed, skipping deploy attempt" >&2
        return 0
    fi

    if ! "$BENCH_BIN" export-web --require-complete --data-dir "$DATA_DIR" --output-dir "$WEB_DATA_DIR"; then
        echo "Web export failed, skipping deploy attempt" >&2
        return 0
    fi

    if ! snapshot_id=$(jq -r '.snapshot_id // empty' "$WEB_DATA_DIR/index.json" 2>/dev/null); then
        echo "Failed to read snapshot_id, skipping deploy attempt" >&2
        return 0
    fi
    if [[ -z "$snapshot_id" ]]; then
        echo "No snapshot_id found in web export index, skipping deploy attempt" >&2
        return 0
    fi

    last_snapshot_id=""
    if [[ -f "$LAST_DEPLOY_SNAPSHOT_FILE" ]] && ! read -r last_snapshot_id < "$LAST_DEPLOY_SNAPSHOT_FILE"; then
        last_snapshot_id=""
    fi
    if [[ "$snapshot_id" == "$last_snapshot_id" ]]; then
        echo "Snapshot '$snapshot_id' already deployed, skipping deploy attempt" >&2
        return 0
    fi

    set +u
    # shellcheck source=/dev/null
    source "$CLOUDFLARE_ENV_FILE"
    set -u

    if [[ -z "${CLOUDFLARE_API_TOKEN:-}" || -z "${CLOUDFLARE_ACCOUNT_ID:-}" || -z "${CF_PAGES_PROJECT:-}" ]]; then
        echo "Missing Cloudflare vars (CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, CF_PAGES_PROJECT), skipping deploy attempt" >&2
        return 0
    fi

    export CLOUDFLARE_API_TOKEN CLOUDFLARE_ACCOUNT_ID

    command=(wrangler pages deploy "$WEB_DIST_DIR" --project-name "$CF_PAGES_PROJECT")
    if [[ -n "${CF_PAGES_BRANCH:-}" ]]; then
        command+=(--branch "$CF_PAGES_BRANCH")
    fi

    printf '%s\n' "$now" > "$LAST_DEPLOY_EPOCH_FILE"

    echo "Deploying snapshot '$snapshot_id' to Cloudflare Pages project '$CF_PAGES_PROJECT'" >&2
    if "${command[@]}"; then
        printf '%s\n' "$snapshot_id" > "$LAST_DEPLOY_SNAPSHOT_FILE"
    else
        echo "Cloudflare deploy failed" >&2
    fi
}

restart_self() {
    echo "Restarting script..." >&2
    exec "$SCRIPT_PATH" "${SCRIPT_ARGS[@]}"
}

run_benchmarks() {
    local command start status end elapsed
    command=("$BENCH_BIN" run --new-order timeline-desc --new-limit 512 --rerun-limit 16 --rerun-only-limit 512)
    echo "Running '${command[*]}'" >&2

    start=$(date +%s)

    set +e
    "${command[@]}"
    status=$?
    set -e

    end=$(date +%s)
    elapsed=$(( end - start ))

    echo "Command '${command[*]}' exited with code $status after ${elapsed}s" >&2
}

main() {
    local head_before_pull head_after_pull

    cd "$REPO_DIR"
    head_before_pull="$(git rev-parse --verify HEAD)"
    git pull --ff-only
    head_after_pull="$(git rev-parse --verify HEAD)"
    if [[ "$head_before_pull" != "$head_after_pull" ]]; then
        echo "Repository updated by git pull, restarting script to use latest version" >&2
        exec "$SCRIPT_PATH" "${SCRIPT_ARGS[@]}"
    fi
    run_on_all_cores cargo build --release

    cd "$AOC_RS_BENCH_DIR"
    run_on_all_cores ./build.sh "$DATA_DIR/"
    run_on_all_cores ./config.py "$DATA_DIR/" "$INPUTS_DIR/"

    cd "$REPO_DIR"

    maybe_sync_and_deploy

    run_benchmarks

    restart_self
}

main "$@"
