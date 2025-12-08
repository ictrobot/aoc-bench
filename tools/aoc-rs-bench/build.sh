#!/bin/bash
set -eou pipefail

cd "$(dirname -- "$0")"

if (( $# < 1 )); then
    echo "usage: $0 <data_dir>" >&2
    exit 1
fi

data_dir="$1"
if [[ ! -d "$data_dir" ]]; then
    echo "error: '$data_dir' does not exist" >&2
    exit 1
fi

builds_dir="$data_dir/builds"
builds_commit_list="$builds_dir/commits.txt"
builds_checksum_file="$builds_dir/checksum.txt"
repo="https://github.com/ictrobot/aoc-rs"
start_ref="main"

# Oldest first, checked in reverse order
framework_revisions=(
    "7189784717f71bba0260b4de3a7cc8d27c116054" # glue-v0 Initial
    "b9c2f7faf8da1c0b73dc2afb8c1d7466f606cb39" # glue-v1 Rename InvalidInputError to InputError
    "9676a84981560f110728475366b9170c47972884" # glue-v2 Multiversion support
    "2ee87f5e33a26a1fc99237c49120214a568c014c" # glue-v3 Multithreading support
    "ab1e77c27919360630046d825583af16a1423d16" # glue-v4 Year 2016 added
    "6b4e6580aa9aaa771e36c4fb214fdc56099e041d" # glue-v5 Year crates reexported from aoc, nested macro repeats for year and day
)

profiles=(
    "generic"
    "native"
    # LTO profiles have been disabled in favour of building per benchmark binaries as doing both would be very slow
    # "generic_lto"
    # "native_lto"
)

# Changes to these files will rebuild all the binaries
checksum_files=(
    "src/lib.rs"
    "Cargo.toml"
    "build.sh"
    "rustc-wrapper-strip-metadata.sh"
)
# Files/folders to delete inside builds_dir on hash change
checksum_delete=(
    "${profiles[@]}"
    "commits.txt"
    ".cache.json" # config.py cache
)

check_checksum() {
    current_checksum="$(sha256sum "${checksum_files[@]}")"
    if [[ -f "$builds_checksum_file" ]]; then
      saved_checksum="$(<"$builds_checksum_file")"
    else
      saved_checksum=""
    fi

    if [[ "$current_checksum" != "$saved_checksum" ]]; then
        echo "Checksum changed, deleting existing builds"

        for f in "${checksum_delete[@]}"; do
            target="${builds_dir:?builds dir must be non-empty}/${f:?file must be non-empty}"
            if [[ -e "$target" ]]; then
                echo "Deleting $target"
                rm -rf -- "${target:?target must be non-empty}"
            fi
        done

        echo "$current_checksum" > "$builds_checksum_file"
    else
        echo "Checksum matches, keeping existing builds"
    fi
}

build_binary() (
    # (...) used instead of {...} so the following commands always run in a subshell, allowing it to have a separate
    # trap EXIT command

    # Create a temporary copy of aoc-rs-bench, used to dynamically generate binary targets for each of the benchmarks.
    # Generating one binary per benchmark seems to significantly decrease commit-to-commit layout variance by ensuring
    # each executable only includes functions needed for that benchmark.
    tmp_crate="$(mktemp -d)"
    trap 'rm -rf "$tmp_crate"' EXIT
    mkdir -p "$tmp_crate/src/bin"
    cp "src/lib.rs" "$tmp_crate/src/lib.rs"
    cp "Cargo.toml" "$tmp_crate/Cargo.toml"
    cp "rustc-wrapper-strip-metadata.sh" "$tmp_crate/rustc-wrapper-strip-metadata.sh"
    cd "$tmp_crate"

    rust_version="$(
        git -C "$tmp_clone" show "$commit:Cargo.toml" \
          | sed -n 's/^[[:space:]]*rust-version[[:space:]]*=[[:space:]]*"\([0-9]\+\.[0-9]\+\).*/\1/p' \
          | head -n1
    )"
    rust_version="${rust_version:-1.75}"

    framework_version=""
    for (( i=${#framework_revisions[@]}-1 ; i>=0 ; i-- )); do
        if git -C "$tmp_clone" merge-base --is-ancestor "${framework_revisions[i]}" "$commit"; then
            framework_version="$i"
            break
        fi
    done
    if [[ -z "$framework_version" ]]; then
        echo "failed to find framework version for commit $commit" 1>&2
        exit 1
    fi

    binaries=0
    while read -r path; do
        if [[ $path =~ ^crates/year([0-9]{4})/src/day([0-9]{2})\.rs$ ]]; then
            year="${BASH_REMATCH[1]}"
            day="${BASH_REMATCH[2]}"
            echo "::aoc_rs_bench::main!{year$year Day$day}" > "$tmp_crate/src/bin/$year-$day.rs"
            binaries=$((binaries + 1))
        fi
    done < <(git -C "$tmp_clone" ls-tree -r --name-only "$commit")

    if (( binaries == 0 )); then
        echo "Failed to create individual benchmark executables for $commit" 1>&2
        exit 1
    fi

    echo
    echo "Building $binaries binaries for $build with rust $rust_version and glue v$framework_version"

    args=(
        "--features" "glue-v$framework_version"
    )

    # All the direct dependencies must be patched
    patch_crates=(
        "aoc"
        "utils"
        "year2015"
    )
    if [[ "$framework_version" -gt 3 ]]; then
        patch_crates+=("year2016")
    fi

    # Patch each direct dependency to use the temporary clone.
    # Using path dependencies for the patches makes it easier to use `remap-path-prefix` compared to using git
    # dependencies (where the checkout is a path inside ~/.cargo), but does cause each crate to have an unstable
    # SourceId due to the `$tmp_clone` path being different each build.
    # However, this is mitigated by the RUSTC_WRAPPER (see below).
    git -C "$tmp_clone" checkout "$commit" -q
    for crate in "${patch_crates[@]}"; do
        args+=(
            "--config" "patch.\"$repo\".$crate.path = \"$tmp_clone/crates/$crate\""
        )
    done

    # Inject a wrapper around cargo's rustc calls to strip all "-C metadata=" arguments.
    #
    # Without this the crate's metadata hash and the disambiguator used in symbol names will change based on the
    # revision of git dependencies or the (temporary) path of path dependencies, which then causes symbol ordering and
    # code layout differences even with zero code changes, which causes measurable performance differences.
    #
    # This problem can be reproduced manually by setting RUSTFLAGS='-C metadata=$x' when building the runner.
    # For example, when benchmarking 2025-01 with the following config:
    #     build=generic,commit=ec46015ca70754859e23d853113ef8682a1b0c92,multiversion=default,threads=1
    # building eight executables with metadata=0 to metadata=7 produces mean runtimes ranging from ~24,000ns and
    # ~28,000ns on an i5-13500 test system (running on isolated P cores). This is significantly more than the variance
    # when rerunning or rebuilding the binary with the same metadata value, which is ~500-1000ns at most.
    #
    # perf shows that all binaries retire almost identical number of instructions (<0.1% different) but have large
    # differences in bad speculation (ranging between ~20-40%) and frontend bound (~20-35%) metrics. cachegrind reports
    # identical instructions across all the binaries.
    #
    # Disassembling the binaries confirms the hot functions are identical apart from differences in relative and
    # absolute addresses and the function ordering. These small layout shifts therefore seem sufficient to cause large
    # knock-on effects at a microarchitecture level.
    #
    # This isn't safe in general, but seems to work fine here where the set of dependencies is known and there's no
    # duplicates with e.g. different versions.
    export RUSTC_WRAPPER='./rustc-wrapper-strip-metadata.sh'

    # Remap paths to $tmp_clone and $tmp_crate to avoid the temporary paths being included in binaries
    export RUSTFLAGS="--remap-path-prefix $tmp_clone=/aoc-rs/ --remap-path-prefix $tmp_crate=/aoc-rs-bench/"

    if [[ "$profile" == "native"* ]]; then
        export RUSTFLAGS="$RUSTFLAGS -C target_cpu=native"
    fi

    if [[ "$profile" == *"_lto" ]]; then
        export CARGO_PROFILE_RELEASE_LTO="fat"
    else
        export CARGO_PROFILE_RELEASE_LTO="false"
    fi

    # Copy output executables into temporary dir, then atomically rename dir when complete
    mkdir -p "$build.tmp"

    while read -r executable; do
        if [[ -z "$executable" || ! -f "$executable" || ! -x "$executable" ]]; then
            echo "No such binary $executable after building $commit" >&2
            exit 1
        fi

        if [[ $executable =~ /release/([0-9]{4})-([0-9]{2})$ ]]; then
            year="${BASH_REMATCH[1]}"
            day="${BASH_REMATCH[2]}"
            mv "$executable" "$build.tmp/$year-$day"
            binaries=$((binaries - 1))
        else
            echo "Unknown binary $executable when building $commit" 1>&2
            exit 1
        fi
    done < <(
        rustup run --install "$rust_version" cargo build --release "${args[@]}" --message-format=json-render-diagnostics \
          | jq --unbuffered -r 'select(.reason=="compiler-artifact" and .executable) | .executable'
    )

    if (( binaries != 0 )); then
        echo "Incorrect number of binaries when building $commit" 1>&2
        exit 1
    fi

    mv "$build.tmp" "$build"
)

mkdir -p "$builds_dir"
check_checksum

tmp_clone="$(mktemp -d)"
trap 'rm -rf "$tmp_clone"' EXIT
git -C "$tmp_clone" clone "$repo" . -q

git -C "$tmp_clone" rev-list --topo-order --reverse "$start_ref" > "$builds_commit_list.tmp"
mv "$builds_commit_list.tmp" "$builds_commit_list"

for profile in "${profiles[@]}"; do
    mkdir -p "$builds_dir/$profile"

    while read -r commit <&3; do
        build="$builds_dir/$profile/$commit"
        if [[ ! -d "$build" ]]; then
            build_binary
        fi
    done 3< "$builds_commit_list"
done
