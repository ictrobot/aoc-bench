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

The i5-13500 has a 117W burst power limit (PL2) and 65W sustained power limit (PL1) after ~8s. Multithreaded benchmarks
use a 10s minimum warmup to reach steady state before measurement.

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
