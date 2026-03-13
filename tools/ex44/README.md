# EX44 Benchmark Host Setup

This folder documents my current [Hetzner EX44](https://www.hetzner.com/dedicated-rootserver/ex44/) benchmark runner
configuration. It is specific to my setup and is shared as a reference, not a general-purpose template.

`installimage.txt` contains the configuration used for
Hetzner's [installimage](https://docs.hetzner.com/robot/dedicated-server/operating-systems/installimage/) system.

## Power limits

The i5-13500 has a 117W burst power limit (PL2) and 65W sustained power limit (PL1) after ~8s.
Multithreaded benchmarks use a 10s minimum warmup to reach steady state before measurement.

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
