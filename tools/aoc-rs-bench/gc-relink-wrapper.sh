#!/bin/bash
# Garbage-collect the rlibs before the final link, see build.sh.
#
# Links once as normal, prunes each rlib's object down to the symbols that survived that link with ld.bfd -r
# --gc-sections, and links again with the pruned rlibs. Codegen is untouched: the same functions are linked at the
# same sizes, and only what the removed code occupied moves. ld.bfd is used as it rewrites .eh_frame for relocatable
# links, which objcopy cannot, and lld refuses -r together with --gc-sections. The second link must keep every
# function of the first at the same size.
#
# rustc runs this in place of the linker with the linker's full arguments. Everything that selects the real linker is
# in those arguments (from rust 1.90, -fuse-ld=lld and a -B to the toolchain's rust-lld shims, before that nothing)
# and is passed through. The file name must not look like a known linker (gcc, clang, ld, lld), or rustc infers a
# linker flavor from it and stops adding those arguments. Pruning drops the objects' address-significance tables,
# so identical code folding is refused rather than let --icf=safe silently fold less in the pruned crates.
#
# AOC_BENCH_GC_LINKER  the linker driver to run, default cc, rustc's default for this target
# AOC_BENCH_GC_CACHE   directory for cached symbol lists and pruned objects, keyed by content hash, kept across links
set -euo pipefail

# Rlibs not worth scanning, matched against the full path.
skip_patterns=(
    # The toolchain's crates never change between commits on the same toolchain.
    "*/lib/rustlib/*/lib/*.rlib"
    # The generated main calls its year crate directly, so aoc is never pulled into the link.
    "*/libaoc-*.rlib"
    # Everything in the bench runner library is live, and changing it changes build.sh's checksum anyway.
    "*/libaoc_rs_bench-*.rlib"
    # Year crates are scanned as they can contain shared modules, like 2019's intcode, with unused functions.
)

# Global symbols an object or rlib defines. DW.ref.rust_eh_personality, the pointer to the personality routine that
# LLVM emits in every object with unwind tables, is ignored throughout as by name it would make every rlib look
# partly live, including ones the link never pulled in. It is the only weak symbol the workspace crates define, and
# the only one every object shares. nm warns about the rlibs' metadata members having no symbols, which is expected.
defined_globals() {
    nm -g --defined-only "$1" 2>/dev/null | awk 'NF == 3 && $3 !~ /^DW\.ref\./ {print $3}' | sort -u
}

# Every function symbol with its size, for comparing the two links.
functions() {
    nm -S --defined-only "$1" 2>/dev/null | awk 'NF == 4 && $3 ~ /^[tTwW]$/ {print $2, $4}' | sort
}

# Writes stdin to a cache entry. The links of a batch run in parallel and may compute the same entry at once, so it
# is written to a temporary file and linked into place, which fails harmlessly if another link got there first. An
# entry is therefore never replaced once visible, and readers never see one change under them.
cache_put() {
    local file
    file="$(mktemp -p "$(dirname "$1")")"
    cat > "$file"
    ln "$file" "$1" 2>/dev/null || true
    rm -f "$file"
}

# The symbol list of an rlib, cached by its content hash. Every link in a commit sees the same utils rlib, and an
# unchanged day produces the same year rlib at every commit, so one scan serves all of them.
rlib_symbols() { # <rlib> <content hash> <out file>
    local key
    if [[ -z "$cache" ]]; then
        defined_globals "$1" > "$3"
        return
    fi
    key="$cache/symbols/$2"
    if [[ ! -f "$key" ]]; then
        defined_globals "$1" | cache_put "$key"
    fi
    cp "$key" "$3"
}

# Prunes the object of an rlib down to the roots, cached by the rlib's content and the root list. utils is unchanged
# at most commits and a day's roots change only when the day does, so most links hit the cache.
prune_rlib() { # <arg index> <rlib>
    local i="$1" rlib="$2" dir="$tmp/$1" hash member key="" roots
    mkdir -p "$dir"
    hash="$(sha256sum < "$rlib" | cut -c1-32)"
    rlib_symbols "$rlib" "$hash" "$dir/defined"
    comm -12 "$dir/defined" "$tmp/live" > "$dir/roots"
    # Nothing live means the rlib was not pulled in, nothing dead means there is nothing to remove
    if [[ ! -s "$dir/roots" ]] || cmp -s "$dir/defined" "$dir/roots"; then
        return
    fi
    # The bench profile compiles every crate as one codegen unit, so an rlib holds exactly one object
    member="$(ar t "$rlib" | grep '\.o$')"
    if [[ "$(echo "$member" | wc -l)" != 1 ]]; then
        echo "gc-relink: expected one object in $rlib, found: $member" 1>&2
        exit 1
    fi
    if [[ -n "$cache" ]]; then
        key="$cache/pruned/$(cat <(echo "$hash") "$dir/roots" | sha256sum | cut -c1-32)"
    fi
    if [[ -n "$key" && -f "$key" ]]; then
        cp "$key" "$dir/$member"
    else
        ar p "$rlib" "$member" > "$dir/in.o"
        # bfd does not understand LLVM's address significance table, which only feeds ICF
        objcopy --remove-section=.llvm_addrsig "$dir/in.o" "$dir/stripped.o"
        # One --undefined=<symbol> argument per root. xargs would split a long list over several ld invocations.
        mapfile -t roots < "$dir/roots"
        ld.bfd -r --gc-sections "${roots[@]/#/--undefined=}" -o "$dir/$member" "$dir/stripped.o"
        if [[ -n "$key" ]]; then
            cache_put "$key" < "$dir/$member"
        fi
    fi
    cp "$rlib" "$dir/lib.rlib"
    ar d "$dir/lib.rlib" "$member"
    (cd "$dir" && ar q lib.rlib "$member")
    touch "$dir/changed"
}

linker="${AOC_BENCH_GC_LINKER:-cc}"
cache="${AOC_BENCH_GC_CACHE:-}"
if [[ -n "$cache" ]]; then
    mkdir -p "$cache/symbols" "$cache/pruned"
fi
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

args=("$@")
out=""
for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ "${args[$i]}" == @* ]]; then
        echo "gc-relink: response files are not supported" 1>&2
        exit 1
    fi
    if [[ "${args[$i]}" == *--icf=* && "${args[$i]}" != *--icf=none* ]]; then
        echo "gc-relink: --icf is not supported as pruning drops .llvm_addrsig" 1>&2
        exit 1
    fi
    if [[ "${args[$i]}" == "-o" ]]; then
        out="${args[$((i+1))]}"
    fi
done
if [[ -z "$out" ]]; then
    echo "gc-relink: no -o argument" 1>&2
    exit 1
fi

"$linker" "$@"

# Defined symbols that survived the first link. Everything an rlib defines that is in this list is a GC root.
nm --defined-only "$out" 2>/dev/null | awk 'NF == 3 && $3 !~ /^DW\.ref\./ {print $3}' | sort -u > "$tmp/live"
before="$(functions "$out")"

workers=()
for ((i = 0; i < ${#args[@]}; i++)); do
    rlib="${args[$i]}"
    if [[ "$rlib" != *.rlib || ! -f "$rlib" ]]; then
        continue
    fi
    skip=0
    for pattern in "${skip_patterns[@]}"; do
        if [[ "$rlib" == $pattern ]]; then
            skip=1
            break
        fi
    done
    if (( skip )); then
        continue
    fi
    prune_rlib "$i" "$rlib" &
    workers+=("$!")
done
# A bare wait would discard the workers' exit statuses, and a failed or partial prune must fail the link
for worker in "${workers[@]}"; do
    if ! wait "$worker"; then
        echo "gc-relink: pruning failed for $out" 1>&2
        exit 1
    fi
done

pruned=0
for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ -e "$tmp/$i/changed" ]]; then
        args[$i]="$tmp/$i/lib.rlib"
        pruned=$((pruned + 1))
    fi
done
if (( pruned == 0 )); then
    exit 0
fi

"$linker" "${args[@]}"

# Pruning may only remove things, so every function the first link kept must still be there at the same size.
after="$(functions "$out")"
if [[ "$before" != "$after" ]]; then
    echo "gc-relink: function symbols differ between the first and second link of $out" 1>&2
    diff <(echo "$before") <(echo "$after") | head -20 1>&2 || true
    exit 1
fi
