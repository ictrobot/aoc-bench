import { describe, expect, it } from "vitest"
import type { HostIndex, IndexedHistory, IndexedResults } from "./types.ts"
import { decodeHistory, decodeLatestResults, decodeResults, decodeResultsForBenchmark } from "./api.ts"

function makeHostIndex(): HostIndex {
  return {
    last_updated: 1_700_000_000,
    // Intentionally unsorted key insertion order to verify decoder sorts key names.
    config_keys: {
      input: { values: ["small", "large"] },
      compiler: { values: ["stable", "nightly"] },
    },
    benchmarks: [
      { name: "bench-a", result_count: 2 },
      { name: "bench-b", result_count: 2 },
    ],
    timeline_key: "compiler",
    results_path: "results.json",
    history_dir: "history",
  }
}

describe("api decode helpers", () => {
  it("decodeResults maps bench/config indices and reuses config objects for repeated indices", () => {
    const index = makeHostIndex()
    const data: IndexedResults = {
      // With sorted keys [compiler, input]:
      // config_idx 2 => { compiler: nightly, input: small }
      // config_idx 1 => { compiler: stable, input: large }
      results: [
        [0, 2, 100, 5],
        [1, 2, 110, 6],
        [0, 1, 120, 7],
      ],
    }

    const decoded = decodeResults(index, data)

    expect(decoded).toEqual([
      {
        bench: "bench-a",
        config: { compiler: "nightly", input: "small" },
        mean_ns: 100,
        ci95_half_ns: 5,
      },
      {
        bench: "bench-b",
        config: { compiler: "nightly", input: "small" },
        mean_ns: 110,
        ci95_half_ns: 6,
      },
      {
        bench: "bench-a",
        config: { compiler: "stable", input: "large" },
        mean_ns: 120,
        ci95_half_ns: 7,
      },
    ])

    // Same config_idx should point to the same cached object instance.
    expect(decoded[0].config).toBe(decoded[1].config)
  })

  it("decodeResultsForBenchmark filters rows to the requested benchmark", () => {
    const index = makeHostIndex()
    const data: IndexedResults = {
      results: [
        [0, 0, 100, 5],
        [1, 3, 200, 10],
        [1, 1, 210, 11],
      ],
    }

    const decoded = decodeResultsForBenchmark(index, data, "bench-b")

    expect(decoded).toEqual([
      {
        bench: "bench-b",
        config: { compiler: "nightly", input: "large" },
        mean_ns: 200,
        ci95_half_ns: 10,
      },
      {
        bench: "bench-b",
        config: { compiler: "stable", input: "large" },
        mean_ns: 210,
        ci95_half_ns: 11,
      },
    ])
  })

  it("decodeResultsForBenchmark returns [] for unknown benchmark name", () => {
    const index = makeHostIndex()
    const data: IndexedResults = {
      results: [[0, 0, 100, 5]],
    }

    expect(decodeResultsForBenchmark(index, data, "does-not-exist")).toEqual([])
  })

  it("decodeLatestResults returns null when latest_results is absent", () => {
    const index = makeHostIndex()
    expect(decodeLatestResults(index)).toBeNull()
  })

  it("decodeLatestResults decodes rows when latest_results is present", () => {
    const index = makeHostIndex()
    index.latest_results = [[1, 2, 321, 9]]

    expect(decodeLatestResults(index)).toEqual([
      {
        bench: "bench-b",
        config: { compiler: "nightly", input: "small" },
        mean_ns: 321,
        ci95_half_ns: 9,
      },
    ])
  })

  it("decodeHistory filters by encoded config index and maps output fields", () => {
    const index = makeHostIndex()
    const data: IndexedHistory = {
      series: [
        [0, 1000, 100, 5, 10],
        [3, 2000, 200, 7, 11],
        [3, 3000, 210, 8, 12],
      ],
    }

    const decoded = decodeHistory(index, data, {
      compiler: "nightly",
      input: "large",
    })

    expect(decoded).toEqual([
      {
        config: { compiler: "nightly", input: "large" },
        timestamp: 2000,
        median_run_mean_ns: 200,
        median_run_ci95_half_ns: 7,
        run_count: 11,
      },
      {
        config: { compiler: "nightly", input: "large" },
        timestamp: 3000,
        median_run_mean_ns: 210,
        median_run_ci95_half_ns: 8,
        run_count: 12,
      },
    ])
  })

  it("decodeHistory returns [] when requested config cannot be encoded", () => {
    const index = makeHostIndex()
    const data: IndexedHistory = {
      series: [[0, 1000, 100, 5, 10]],
    }

    expect(
      decodeHistory(index, data, {
        compiler: "nightly",
        input: "unknown",
      }),
    ).toEqual([])
  })

  it("throws on invalid row indices in results and history payloads", () => {
    const index = makeHostIndex()

    expect(() =>
      decodeResults(index, {
        results: [[5, 0, 100, 5]],
      }),
    ).toThrow(/Invalid bench_idx/)

    expect(() =>
      decodeHistory(
        index,
        {
          series: [[999, 1000, 100, 5, 10]],
        },
        { compiler: "stable", input: "small" },
      ),
    ).toThrow(/Invalid config_idx/)
  })
})
