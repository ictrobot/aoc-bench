#!/bin/bash
set -euo pipefail

# Strip "-C metadata=..." and "-Cmetadata=..." from the arguments to REAL_RUSTC
# See build.sh for details

REAL_RUSTC="$1"
shift 1

ARGS=()
REMOVED_ARGS=()

while (($#)); do
    case "$1" in
        -C)
            if (($# >= 2)) && [[ "$2" == metadata=* ]]; then
                REMOVED_ARGS+=("$1" "$2")
                shift 2
                continue
            else
                ARGS+=("$1")
                shift
                continue
            fi
            ;;
        -Cmetadata=*)
            REMOVED_ARGS+=("$1")
            shift
            continue
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -n "${DEBUG_WRAPPER:-}" ]] && ((${#REMOVED_ARGS[@]} > 0)); then
    echo "Removed arguments: ${REMOVED_ARGS[*]}" >&2
fi

exec "$REAL_RUSTC" "${ARGS[@]}"
