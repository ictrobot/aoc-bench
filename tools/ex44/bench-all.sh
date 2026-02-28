#!/bin/bash
set -eou pipefail
set -x

SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"
SCRIPT_ARGS=("$@")

# Environment-specific paths
REPO_DIR="${AOC_BENCH_DIR:-$HOME/aoc-bench}"
AOC_RS_BENCH_DIR="$REPO_DIR/tools/aoc-rs-bench"
DATA_DIR="$REPO_DIR/data"
CONFIG_JSON="$DATA_DIR/config.json"
INPUTS_DIR="${AOC_INPUTS_DIR:-$HOME/inputs}"
BENCH_BIN="target/release/aoc-bench"

run_benchmarks() {
    local command start status end elapsed
    command=("$BENCH_BIN" run --rerun-limit 0 --new-limit 1000 "$@")
    echo "Running '${command[*]}'" >&2

    start=$(date +%s)

    set +e
    "${command[@]}"
    status=$?
    set -e

    end=$(date +%s)
    elapsed=$(( end - start ))

    echo "Command '${command[*]}' exited with code $status after ${elapsed}s" >&2

    if (( elapsed > 60 )); then
        # Clean up file descriptors before restart
        set +e
        exec 3<&-
        exec 4<&-
        exec 5<&-
        set -e

        command=("$BENCH_BIN" run --rerun-limit 16 --new-limit 0)
        echo "Rerunning some benchmarks before restarting" >&2
        echo "Running '${command[*]}'" >&2
        "${command[@]}" || true

        echo "Restarting script..." >&2
        exec "$SCRIPT_PATH" "${SCRIPT_ARGS[@]}"
    fi
}

cd "$REPO_DIR"
git pull --ff-only
cargo build --release

cd "$AOC_RS_BENCH_DIR"
./build.sh "$DATA_DIR/"
./config.py "$DATA_DIR/" "$INPUTS_DIR/"

cd "$REPO_DIR"

# Three streams: latest puzzle, latest commit, and forward puzzle pass.
exec 3< <(jq -r '.benchmarks | reverse | .[].benchmark' "$CONFIG_JSON")
exec 4< <(jq -r '.config_keys.commit.values | reverse | .[]' "$CONFIG_JSON")
exec 5< <(jq -r '.benchmarks | .[].benchmark' "$CONFIG_JSON" | grep -vP '^(2015-04|2016-05|2016-14)$')

while true; do
    if read -r last_puzzle <&3; then
        run_benchmarks "$last_puzzle"
    fi
    if read -r last_commit <&4; then
        run_benchmarks --config "commit=$last_commit"
    fi
    if read -r next_puzzle <&5; then
        run_benchmarks "$next_puzzle"
    fi

    [[ -z "$last_puzzle" && -z "$last_commit" && -z "$next_puzzle" ]] && break
done

# Clean up loop file descriptors
set +e
exec 3<&-
exec 4<&-
exec 5<&-
set -e

# After reaching a steady state, run a wider rerun batch before restarting.
command=("$BENCH_BIN" run --rerun-limit 256 --new-limit 0)
echo "Up to date, rerunning batch of benchmarks" >&2
echo "Running '${command[*]}'" >&2
"${command[@]}" || true

echo "Restarting script..." >&2
exec "$SCRIPT_PATH" "${SCRIPT_ARGS[@]}"
