# aoc-rs-bench

Harness and build pipeline for benchmarking [aoc-rs](https://github.com/ictrobot/aoc-rs): one binary per puzzle per
commit, built so that only code changes alter the bytes.

* `src/lib.rs`: the harness. `check` prints the answers' checksum and supported variants, `bench <threads>
  <multiversion>` emits `SAMPLE` lines for aoc-bench.
* `build.sh`: build orchestration. `build.sh <data_dir>` builds every upstream commit into `<data_dir>/builds` as the
  hosts do, `build.sh --single <checkout> <year> <day> <profile> <out_dir>` builds one day from a working tree without
  modifying the checkout, and `build.sh --checksum` prints the checksum guarding existing builds.
* `prune.py`: trims each year crate to one day number before a batch is built.
* `normalize_panic_locations.py`: normalizes aoc-rs panic lines and columns so moving code doesn't change binaries.
  Panics report `<file>:1650683745:1751346789` (`b"aocbench"` read as two u32s).
* `rustc-wrapper-strip-metadata.sh`, `layout.ld`: stable symbol names and code layout.
* `config.py`: generates `data/config.json` from the builds and inputs.
* `rbench`: benchmark a local checkout on a benchmark host, see below.

## rbench

```
rbench [-v] <year> <day> [threads] [multiversion]
rbench --status
```

Builds the day with `build.sh --single`, checks the answers locally, pushes the binary and input over SSH to the
host's `aoc-bench-rbench` command and prints the run series it streams back, `--raw` for the JSON. The host pauses
its regular benchmarks for the run and resumes five minutes after the last one. One run at a time. Host side in
`../bench-host/`.

Needs `rsync`, `zstd`, `jq`, `python3`, `rustup`, and binutils for `readelf`, `objcopy`, `nm`, `ar` and `ld.bfd`. `threads` defaults to 1, or `n` for the
binary's default, and `multiversion` to `default`. Options and their environment variable fallbacks:

| option       | variable            | default                       |
|--------------|---------------------|-------------------------------|
| `--profile`  | `RBENCH_PROFILE`    | `generic`, or `native`        |
| `--checkout` | `RBENCH_CHECKOUT`   | `~/aoc-rs`                    |
| `--host`     | `RBENCH_HOST`       | `aoc-bench-rbench`            |
|              | `RBENCH_TARGET_CPU` | `raptorlake`, for `native`    |
|              | `RBENCH_CACHE_DIR`  | `/tmp/aoc-bench-rbench-<uid>` |

`native` targets `raptorlake` rather than the build machine's CPU so the binary matches the host's native builds.

The default host matches an SSH config entry such as:

```
Host aoc-bench-rbench
    HostName <host address>
    User rbench
    IdentityFile ~/.ssh/<key accepted by the rbench user>
    RequestTTY no
```
