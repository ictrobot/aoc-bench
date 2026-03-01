# aoc-bench

Experimental benchmarking tool, primarily designed for [`aoc-rs`](https://github.com/ictrobot/aoc-rs), my Advent of Code
solutions written in Rust.

Designed to continuously benchmark workloads across their entire commit history, accumulating long-term stable data to
compare performance across commits, thread counts, and build configurations.

Built using LLM coding assistants for design, code, and test generation, however, much of it was then
improved/restructured/optimized by hand.

## Links

- [bench.egj.dev](https://bench.egj.dev/) – hosted web frontend showing `aoc-rs` data
- [DESIGN.md](DESIGN.md) – architecture and design spec
- [TASKS.md](TASKS.md) – implementation roadmap and status
- [tools/aoc-rs-bench](tools/aoc-rs-bench/) – `aoc-rs` benchmarking glue and config generation
- [tools/ex44/](tools/ex44/) – benchmark host configuration
