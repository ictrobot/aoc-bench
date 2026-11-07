import type { GlobalIndex, HistoryRow, HostIndex, IndexedHistory, IndexedResults, ResultRow } from "@/lib/types.ts"

export const TEST_HOST = "linux-x64"

type BenchmarkOverride = Omit<HostIndex["benchmarks"][number], "config_keys" | "history_parts"> & {
  config_keys?: number[]
  history_parts?: number[]
}
type HostIndexOverrides = Omit<Partial<HostIndex>, "benchmarks"> & { benchmarks?: BenchmarkOverride[] }

export function makeHostIndex(overrides: HostIndexOverrides = {}): HostIndex {
  const index: HostIndex = {
    last_updated: 1_700_000_000,
    config_keys: {},
    timeline_key: null,
    results_path: "results.json",
    history_dir: "history",
    ...overrides,
    benchmarks: [],
  }
  const allKeyIndexes = Object.keys(index.config_keys)
    .sort()
    .map((_, keyIndex) => keyIndex)
  index.benchmarks = (overrides.benchmarks ?? []).map((benchmark) => ({
    ...benchmark,
    config_keys: benchmark.config_keys ?? allKeyIndexes,
    history_parts: benchmark.history_parts ?? [0],
  }))
  return index
}

function column(rows: (number | null)[][], c: number, delta: boolean): number[] {
  let prev = 0
  return rows.map((row) => {
    const value = row[c]
    if (value === null) throw new Error(`Column ${c} is not nullable`)
    const out = delta ? value - prev : value
    prev = value
    return out
  })
}

function nullableColumn(rows: (number | null)[][], c: number): (number | null)[] {
  return rows.map((row) => row[c])
}

/** Encode result rows the way the exporter writes results.json. */
export function columnarResults(rows: ResultRow[]): IndexedResults {
  return {
    results: {
      bench_idx_delta: column(rows, 0, true),
      config_idx_delta: column(rows, 1, true),
      measurement_token: column(rows, 2, false),
      mean_ns: column(rows, 3, false),
      ci95_half_ns: nullableColumn(rows, 4),
      run_count: column(rows, 5, false),
    },
  }
}

/** Encode history rows the way the exporter writes a history part. */
export function columnarHistory(rows: HistoryRow[]): IndexedHistory {
  return {
    series: {
      config_idx_delta: column(rows, 0, true),
      measurement_token: column(rows, 1, false),
      timestamp_s_delta: column(rows, 2, true),
      mean_ns: column(rows, 3, false),
      ci95_half_ns: nullableColumn(rows, 4),
      run_count: column(rows, 5, false),
    },
  }
}

export function makeGlobalIndex(
  hostIndex: HostIndex,
  {
    host = TEST_HOST,
    snapshotId = "snapshot-a",
  }: {
    host?: string
    snapshotId?: string
  } = {},
): GlobalIndex {
  return {
    schema_version: 6,
    snapshot_id: snapshotId,
    hosts: { [host]: hostIndex },
  }
}
