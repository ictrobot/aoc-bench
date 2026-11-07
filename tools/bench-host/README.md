# Benchmark Host Setup

This folder documents my current benchmark runner configuration. It is specific to my setup and is shared as a
reference, not a general-purpose template.

## ex44

ex44 is the main benchmark host: a [Hetzner EX44](https://www.hetzner.com/dedicated-rootserver/ex44/) server with
an [Intel i5-13500](https://www.intel.com/content/www/us/en/products/sku/230580/intel-core-i513500-processor-24m-cache-up-to-4-80-ghz/specifications.html).
Benchmarks are isolated to the 6 P-cores, leaving the E-cores for the runner process and background tasks.

`ex44-installimage.txt` contains the configuration used for
Hetzner's [installimage](https://docs.hetzner.com/robot/dedicated-server/operating-systems/installimage/) system.

### Power Limits

The i5-13500 has a 117W burst power limit (PL2) and a 65W sustained power limit (PL1), both locked by the BIOS. PL1
is enforced on an exponentially weighted moving average of package power with an 8s time constant, not a timer that
resets when the CPU goes idle, so how long a run can exceed 65W depends on what ran before it. Multithreaded
benchmarks can exceed it and then settle at 65W with lower effective clocks.

The host config sets `power_limit_watts` to PL1, so the runner discards samples until the RAPL package power counter
shows the limit has engaged before warmup starts. `setup.yml` makes the counter readable by the `rapl` group.

```
> powercap-info
intel-rapl
  enabled: 1
  Zone 0
    name: package-0
    enabled: 0
    max_energy_range_uj: 262143328850
    energy_uj: 65997277274
    Constraint 0
      name: long_term
      power_limit_uw: 65000000
      time_window_us: 7995392
      max_power_uw: 65000000
    Constraint 1
      name: short_term
      power_limit_uw: 117000000
      time_window_us: 2440
      max_power_uw: 0
    Zone 0:0
      name: core
      enabled: 0
      max_energy_range_uj: 262143328850
      energy_uj: 48265700414
      Constraint 0
        name: long_term
        power_limit_uw: 0
        time_window_us: 976
    Zone 0:1
      name: uncore
      enabled: 0
      max_energy_range_uj: 262143328850
      energy_uj: 5981
      Constraint 0
        name: long_term
        power_limit_uw: 0
        time_window_us: 976
```

## k8

k8 is designed to benchmark Advent of Code solutions against the [2015 about page](https://adventofcode.com/2015/about)
claim that "every problem has a solution that completes in at most 15 seconds on ten-year-old hardware". The host uses
an [AMD Athlon 64 X2 3800+](https://en.wikipedia.org/wiki/Athlon_64_X2) based on
the [K8 microarchitecture](https://en.wikipedia.org/wiki/AMD_K8).

The specific CPU used is the AM2 version, released in May 2006, which benchmarks within a few percent of the equivalent
Socket 939 Athlon 64 X2 3800+ released in August 2005. The AM2 platform is much more modern and made it easier to find a
reliable motherboard.

For single-core benchmarks, it is roughly equivalent to the Socket 939 Venice revision of the
[Athlon 64 3200+](https://en.wikipedia.org/wiki/Athlon_64), released in April 2005. That makes the single-threaded
numbers comparable to a much more mainstream 2005 CPU, while still giving a high-end dual-core reference for
multithreaded solutions.

## rbench

With `bench_enable_rbench` set, a host accepts one-off benchmark binaries pushed by `tools/aoc-rs-bench/rbench` and
runs them without recording anything. The binaries are trusted: anyone holding an accepted key can run whatever
they build on the host. Installed by `setup.yml`:

* An `rbench` user whose every login runs `/usr/local/libexec/aoc-bench-rbench` via an sshd `Match User` block.
  It accepts the bench user's key, or the keys in `rbench_ssh_pubkeys` if set.
* `aoc-bench-rbench` stops `aoc-bench.service`, receives the upload, verifies the answers and runs the binary under
  `aoc-bench debug`, confined by Landlock to its job directory, in an empty network namespace, with a time limit. A
  lock allows one run at a time.
* A polkit rule allowing `rbench` to stop `aoc-bench.service`.
* `aoc-bench-watchdog.timer`, a root check every minute that restarts `aoc-bench.service` five minutes after the last
  run. A lock held for over an hour is treated as stuck and the user's processes are killed.
* `bench-all.sh` copies the built `aoc-bench` and host config to `/var/lib/aoc-bench` for the runs to use.

`ssh rbench@<host> status` shows the service and lock state. To resume immediately after a cut-off run,
`systemctl start aoc-bench.service`. To disable, set `bench_enable_rbench` to false and rerun the playbook.

## Metrics

With `metrics_push_url` set, the unit passes it to `aoc-bench` as `AOC_BENCH_METRICS_PUSH_URL`, which pushes the
metrics described in section 11.3 of `DESIGN.md`. `bench-all.sh` pushes series of its own to the same URL with curl,
through `metrics.sh`:

* Each stage of a cycle (`aoc_bench_build`, `aoc_rs_bench_build`, `config`, `deploy` and `run`) pushes
  `aoc_bench_stage_start_timestamp_seconds{stage}` when it starts and an `aoc_bench_stage_seconds{stage,outcome}`
  increment when it ends, outcome `ok` or `failed`.
* `build.sh` reports progress through `AOC_BENCH_BUILD_HOOK`, which `bench-all.sh` points at `metrics.sh`. Before
  building, each profile pushes the gauges `aoc_bench_build_commits{profile}` and
  `aoc_bench_build_commits_remaining{profile}`, the latter again after every commit it builds. Each built commit also
  pushes the increments `aoc_bench_build_commit_seconds{profile}` and `aoc_bench_build_binaries{profile,outcome}`,
  outcome `new` for a binary not already in `by-hash` and `reused` otherwise.
