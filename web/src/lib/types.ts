/**
 * A row of results.json or of `latest_results`:
 * `[bench_idx, config_idx, measurement_token, mean_ns, ci95_half_ns]`.
 */
export type ResultRow = [
  bench_idx: number,
  config_idx: number,
  measurement_token: number,
  mean_ns: number,
  ci95_half_ns: number,
]

/** A row of a history part: `[config_idx, measurement_token, timestamp_s, mean_ns, ci95_half_ns, run_count]`. */
export type HistoryRow = [
  config_idx: number,
  measurement_token: number,
  timestamp_s: number,
  mean_ns: number,
  ci95_half_ns: number,
  run_count: number,
]

export interface GlobalIndex {
  schema_version: number
  snapshot_id: string
  hosts: Record<string, HostIndex>
}

export interface HostIndex {
  last_updated: number
  description?: string
  config_keys: Record<string, { values: string[]; annotations?: Record<string, string>; link?: string }>
  benchmarks: {
    name: string
    result_count: number
    config_keys: number[]
    /**
     * First config_idx of each history part file, in part order (always starts at 0, strictly
     * increasing). Part `n` is `history/{name}.{n}.json`.
     */
    history_parts: number[]
  }[]
  timeline_key: string | null
  results_path: string
  history_dir: string
  /** Latest stable results for the most recent timeline key value. */
  latest_results?: ResultRow[]
}

/**
 * results.json: one array per `ResultRow` field, all the same length. `*_delta` columns hold
 * the difference from the previous row's value, starting from zero.
 */
export interface IndexedResults {
  results: {
    bench_idx_delta: number[]
    config_idx_delta: number[]
    measurement_token: number[]
    mean_ns: number[]
    ci95_half_ns: number[]
  }
}

/** history/{bench}.{part}.json: one array per `HistoryRow` field, delta-encoded like `IndexedResults`. */
export interface IndexedHistory {
  series: {
    config_idx_delta: number[]
    measurement_token: number[]
    timestamp_s_delta: number[]
    mean_ns: number[]
    ci95_half_ns: number[]
    run_count: number[]
  }
}

export interface CompactResult {
  bench: string
  config: Record<string, string>
  /** Positive for a shared measurement; zero for an isolated result. */
  measurement_token: number
  mean_ns: number
  ci95_half_ns: number
}

export interface HistorySeries {
  config: Record<string, string>
  /** Positive for a shared measurement; zero for an isolated result. */
  measurement_token: number
  timestamp: number
  median_run_mean_ns: number
  median_run_ci95_half_ns: number
  run_count: number
}
