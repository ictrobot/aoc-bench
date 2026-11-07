#!/bin/bash
# Metrics for the stages of bench-all.sh and the progress of tools/aoc-rs-bench/build.sh, pushed with curl.
# With AOC_BENCH_METRICS_PUSH_URL unset every function here does nothing. A failed push prints a warning.
#
# bench-all.sh sources this file. Run directly, the arguments are a build.sh hook event, so bench-all.sh also passes
# this file as AOC_BENCH_BUILD_HOOK.

METRICS_URL="${AOC_BENCH_METRICS_PUSH_URL:-}"
METRICS_HOST="${BENCH_HOST:-$(hostname)}"

# The stage in progress and when it started, in microseconds
METRICS_STAGE=""
METRICS_STAGE_START_US=0

metrics_enabled() {
    if [[ -z $METRICS_URL ]]; then
        return 1
    fi
    if [[ ! $METRICS_HOST =~ ^[A-Za-z0-9._-]+$ ]]; then
        echo "Invalid host '$METRICS_HOST', nothing pushed" >&2
        return 1
    fi
}

# Microseconds as decimal seconds
metrics_seconds() {
    printf '%d.%06d\n' "$(( $1 / 1000000 ))" "$(( $1 % 1000000 ))"
}

# One sample: name, the labels after host as name="value" pairs joined by commas, value and time in microseconds
metrics_line() {
    printf '%s{host="%s"%s} %s %s\n' "$1" "$METRICS_HOST" "${2:+,$2}" "$3" "$(( $4 / 1000 ))"
}

# Push sample lines, one per argument. Two retries 5 s apart ride out the agent being unreachable for about 10 s,
# such as during a restart. A retry resends the same timestamps, which the backend's deduplication drops.
metrics_push() {
    if ! metrics_enabled; then
        return 0
    fi
    if ! printf '%s\n' "$@" \
        | curl --silent --show-error --fail --max-time 5 --retry 2 --retry-delay 5 --retry-connrefused \
            --data-binary @- "$METRICS_URL"; then
        echo "Metrics push failed" >&2
    fi
}

# Start a stage. Any stage still in progress should have been ended first. An invalid stage only prints a warning,
# as a failure here would stop the batch.
metrics_stage_start() {
    local stage="${1:-}" now_us
    if [[ ! $stage =~ ^[a-z][a-z_]*$ ]]; then
        echo "Invalid stage '$stage', nothing pushed" >&2
        return 0
    fi

    now_us="$(date +%s%6N)"
    METRICS_STAGE="$stage"
    METRICS_STAGE_START_US="$now_us"
    metrics_push "$(metrics_line aoc_bench_stage_start_timestamp_seconds "stage=\"$stage\"" \
        "$(metrics_seconds "$now_us")" "$now_us")"
}

# End the stage in progress, failed if the exit status given is not 0. Does nothing without a stage in progress.
# An invalid status only prints a warning, as for metrics_stage_start.
metrics_stage_end() {
    local status="${1:-0}" outcome now_us
    if [[ -z $METRICS_STAGE ]]; then
        return 0
    fi
    if [[ ! $status =~ ^[0-9]+$ ]]; then
        echo "Invalid status '$status', nothing pushed" >&2
        return 0
    fi

    outcome="ok"
    if [[ "$status" != 0 ]]; then
        outcome="failed"
    fi
    now_us="$(date +%s%6N)"
    metrics_push "$(metrics_line aoc_bench_stage_seconds "stage=\"$METRICS_STAGE\",outcome=\"$outcome\"" \
        "$(metrics_seconds "$(( now_us - METRICS_STAGE_START_US ))")" "$now_us")"
    METRICS_STAGE=""
}

# The build.sh hook's start event, see build_hook in build.sh
metrics_build_start() {
    local profile="${1:-}" commits="${2:-}" remaining="${3:-}" now_us labels
    if [[ ! $profile =~ ^[a-z][a-z_]*$
        || ! $commits =~ ^[0-9]+$
        || ! $remaining =~ ^[0-9]+$ ]]; then
        echo "Invalid start event '$*', nothing pushed" >&2
        return 1
    fi

    now_us="$(date +%s%6N)"
    labels="profile=\"$profile\""
    metrics_push \
        "$(metrics_line aoc_bench_build_commits "$labels" "$commits" "$now_us")" \
        "$(metrics_line aoc_bench_build_commits_remaining "$labels" "$remaining" "$now_us")"
}

# The build.sh hook's built event, see build_hook in build.sh
metrics_build_built() {
    local profile="${1:-}" commit="${2:-}" microseconds="${3:-}" new="${4:-}" reused="${5:-}" remaining="${6:-}"
    local now_us labels
    if [[ ! $profile =~ ^[a-z][a-z_]*$
        || ! $commit =~ ^[0-9a-f]{40}$
        || ! $microseconds =~ ^[0-9]+$
        || ! $new =~ ^[0-9]+$
        || ! $reused =~ ^[0-9]+$
        || ! $remaining =~ ^[0-9]+$ ]]; then
        echo "Invalid built event '$*', nothing pushed" >&2
        return 1
    fi

    now_us="$(date +%s%6N)"
    labels="profile=\"$profile\""
    metrics_push \
        "$(metrics_line aoc_bench_build_commit_seconds "$labels" "$(metrics_seconds "$microseconds")" "$now_us")" \
        "$(metrics_line aoc_bench_build_binaries "$labels,outcome=\"new\"" "$new" "$now_us")" \
        "$(metrics_line aoc_bench_build_binaries "$labels,outcome=\"reused\"" "$reused" "$now_us")" \
        "$(metrics_line aoc_bench_build_commits_remaining "$labels" "$remaining" "$now_us")"
}

# Run as build.sh's hook. Invalid arguments fail the event, which build.sh reports and carries on. Unknown events
# are ignored so build.sh can add more.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    set -eou pipefail
    case "${1:-}" in
        start)
            shift
            metrics_build_start "$@"
            ;;
        built)
            shift
            metrics_build_built "$@"
            ;;
    esac
fi
