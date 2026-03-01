/** A row in results.json: `[bench_idx, config_idx, mean_ns, ci95_half_ns]` */
export type ResultRow = [bench_idx: number, config_idx: number, mean_ns: number, ci95_half_ns: number]

/** A row in history/{bench}.json: `[config_idx, timestamp_s, mean_ns, ci95_half_ns, run_count]` */
export type HistoryRow = [
  config_idx: number,
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
  config_keys: Record<string, { values: string[]; annotations?: Record<string, string> }>
  benchmarks: { name: string; result_count: number }[]
  timeline_key: string | null
  results_path: string
  history_dir: string
  /** Latest stable results for the most recent timeline key value. */
  latest_results?: ResultRow[]
}

export interface IndexedResults {
  results: ResultRow[]
}

export interface IndexedHistory {
  series: HistoryRow[]
}

export interface CompactResult {
  bench: string
  config: Record<string, string>
  mean_ns: number
  ci95_half_ns: number
}

export interface HistorySeries {
  config: Record<string, string>
  timestamp: number
  median_run_mean_ns: number
  median_run_ci95_half_ns: number
  run_count: number
}
