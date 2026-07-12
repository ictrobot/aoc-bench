import { describe, expect, it } from "vitest"
import type { HostIndex, IndexedHistory, IndexedResults } from "./types.ts"
import { decodeHistory, decodeLatestResults, decodeResults, decodeResultsForBenchmark } from "./api.ts"

function makeHostIndex(): HostIndex {
  return {
    last_updated: 1_700_000_000,
    config_keys: {
      input: { values: ["small", "large"] },
      compiler: { values: ["stable", "nightly"] },
    },
    benchmarks: [
      { name: "bench-a", result_count: 2, config_keys: [0, 1] },
      { name: "bench-b", result_count: 2, config_keys: [0] },
    ],
    timeline_key: "compiler",
    results_path: "results.json",
    history_dir: "history",
  }
}

describe("api decode helpers", () => {
  it("decodes benchmark-local config indices and measurement tokens", () => {
    const index = makeHostIndex()
    const data: IndexedResults = {
      results: [
        [0, 2, 7, 100, 5], // bench-a: nightly/small
        [1, 1, 0, 110, 6], // bench-b: nightly (isolated)
        [0, 2, 7, 120, 7],
      ],
    }

    const decoded = decodeResults(index, data)

    expect(decoded).toEqual([
      {
        bench: "bench-a",
        config: { compiler: "nightly", input: "small" },
        measurement_token: 7,
        mean_ns: 100,
        ci95_half_ns: 5,
      },
      {
        bench: "bench-b",
        config: { compiler: "nightly" },
        measurement_token: 0,
        mean_ns: 110,
        ci95_half_ns: 6,
      },
      {
        bench: "bench-a",
        config: { compiler: "nightly", input: "small" },
        measurement_token: 7,
        mean_ns: 120,
        ci95_half_ns: 7,
      },
    ])
    expect(decoded[0].config).toBe(decoded[2].config)
    expect(decoded[0].config).not.toBe(decoded[1].config)
  })

  it("filters rows to the requested benchmark", () => {
    const index = makeHostIndex()
    const data: IndexedResults = {
      results: [
        [0, 0, 1, 100, 5],
        [1, 1, 2, 200, 10],
      ],
    }

    expect(decodeResultsForBenchmark(index, data, "bench-b")).toEqual([
      {
        bench: "bench-b",
        config: { compiler: "nightly" },
        measurement_token: 2,
        mean_ns: 200,
        ci95_half_ns: 10,
      },
    ])
    expect(decodeResultsForBenchmark(index, data, "unknown")).toEqual([])
  })

  it("decodes latest results with benchmark-local configs", () => {
    const index = makeHostIndex()
    expect(decodeLatestResults(index)).toBeNull()
    index.latest_results = [[1, 1, 3, 321, 9]]

    expect(decodeLatestResults(index)).toEqual([
      {
        bench: "bench-b",
        config: { compiler: "nightly" },
        measurement_token: 3,
        mean_ns: 321,
        ci95_half_ns: 9,
      },
    ])
  })

  it("decodes benchmark history and filters by config", () => {
    const index = makeHostIndex()
    const data: IndexedHistory = {
      series: [
        [0, 0, 1000, 100, 5, 10],
        [3, 4, 2000, 200, 7, 11],
        [3, 5, 3000, 210, 8, 12],
      ],
    }

    expect(
      decodeHistory(index, data, "bench-a", {
        compiler: "nightly",
        input: "large",
      }),
    ).toEqual([
      {
        config: { compiler: "nightly", input: "large" },
        measurement_token: 4,
        timestamp: 2000,
        median_run_mean_ns: 200,
        median_run_ci95_half_ns: 7,
        run_count: 11,
      },
      {
        config: { compiler: "nightly", input: "large" },
        measurement_token: 5,
        timestamp: 3000,
        median_run_mean_ns: 210,
        median_run_ci95_half_ns: 8,
        run_count: 12,
      },
    ])
    expect(decodeHistory(index, data, "bench-a", { compiler: "unknown", input: "large" })).toEqual([])
  })

  it("rejects malformed benchmark, config, and key indices", () => {
    const index = makeHostIndex()
    expect(() => decodeResults(index, { results: [[5, 0, 1, 100, 5]] })).toThrow(/Invalid bench_idx/)
    expect(() =>
      decodeHistory(index, { series: [[999, 1, 1000, 100, 5, 10]] }, "bench-a", {
        compiler: "stable",
        input: "small",
      }),
    ).toThrow(/Invalid config_idx/)

    index.benchmarks[0].config_keys = [99]
    expect(() => decodeResults(index, { results: [[0, 0, 1, 100, 5]] })).toThrow(/Invalid config key index/)
  })
})
