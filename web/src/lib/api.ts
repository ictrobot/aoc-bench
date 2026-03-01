import type { QueryClient } from "@tanstack/react-query"
import type {
  CompactResult,
  GlobalIndex,
  HistorySeries,
  HostIndex,
  IndexedHistory,
  IndexedResults,
  ResultRow,
} from "./types.ts"

const BASE = import.meta.env.BASE_URL + "data"

export class SnapshotNotFoundError extends Error {
  constructor(path: string) {
    super(`Snapshot data not found: ${path}`)
    this.name = "SnapshotNotFoundError"
  }
}

/**
 * Create a TanStack Query retry handler that recovers from stale snapshot misses.
 *
 * When a `SnapshotNotFoundError` occurs, the handler invalidates the `["index"]`
 * query so a fresh index (with updated snapshot paths) is fetched, then allows
 * one retry. A guard prevents repeated invalidation for the same snapshot ID.
 */
export function createSnapshotRetry(
  queryClient: QueryClient,
  onRecovery?: () => void,
): (failureCount: number, error: Error) => boolean {
  let lastInvalidatedSnapshot: string | undefined
  return (failureCount: number, error: Error): boolean => {
    if (error instanceof SnapshotNotFoundError) {
      const currentSnapshot = queryClient.getQueryData<{ snapshot_id: string }>(["index"])?.snapshot_id
      if (currentSnapshot && currentSnapshot !== lastInvalidatedSnapshot) {
        lastInvalidatedSnapshot = currentSnapshot
        queryClient.invalidateQueries({ queryKey: ["index"] })
        onRecovery?.()
      }
      return failureCount < 1
    }
    return failureCount < 3
  }
}

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}/${path}`)
  const snapshotPath = path.includes("snapshots/")
  if (snapshotPath && res.status === 404) {
    throw new SnapshotNotFoundError(path)
  }
  if (!res.ok) {
    throw new Error(`Failed to fetch ${path}: ${res.status} ${res.statusText}`)
  }
  const contentType = res.headers.get("content-type")?.toLowerCase() ?? ""
  if (snapshotPath && contentType.includes("text/html")) {
    // Unknown path rewritten to index.html
    throw new SnapshotNotFoundError(path)
  }
  return res.json() as Promise<T>
}

/** Load the top-level host index metadata. */
export function loadIndex(): Promise<GlobalIndex> {
  return fetchJson<GlobalIndex>("index.json")
}

/** Load the compact indexed results payload for one host. */
export function loadResults(index: HostIndex): Promise<IndexedResults> {
  return fetchJson<IndexedResults>(index.results_path)
}

/** Load the compact history payload for one benchmark on one host. */
export function loadHistory(index: HostIndex, bench: string): Promise<IndexedHistory> {
  return fetchJson<IndexedHistory>(`${index.history_dir}/${bench}.json`)
}

/** Extract ordered config key names and per-key value arrays from the index. */
function indexConfigTables(index: HostIndex): { keys: string[]; values: string[][] } {
  // config_keys is encoded as an object; sort key names for deterministic mixed-radix decode.
  const keys = Object.keys(index.config_keys).sort()
  const values = keys.map((k) => index.config_keys[k].values)
  return { keys, values }
}

function decodeConfig(idx: number, configKeys: string[], configValues: string[][]): Record<string, string> {
  // Decode from last key to first (mixed-radix: last key has stride 1), but
  // collect into an indexed array so we can insert into the result object in
  // forward (alphabetical) key order, matching the sorted configKeys array.
  const values: string[] = new Array(configKeys.length)
  for (let i = configKeys.length - 1; i >= 0; i--) {
    const dim = configValues[i].length
    values[i] = configValues[i][idx % dim]
    idx = Math.floor(idx / dim)
  }
  const result: Record<string, string> = {}
  for (let i = 0; i < configKeys.length; i++) {
    result[configKeys[i]] = values[i]
  }
  return result
}

function encodeConfig(config: Record<string, string>, configKeys: string[], configValues: string[][]): number {
  let idx = 0
  for (let i = 0; i < configKeys.length; i++) {
    const valIdx = configValues[i].indexOf(config[configKeys[i]])
    if (valIdx === -1) return -1
    idx = idx * configValues[i].length + valIdx
  }
  return idx
}

/**
 * Cache decoded config objects by config_idx. Many results share the same config
 * combination, so this avoids creating duplicate objects for the same index.
 * Uses a sparse array since config indices are sequential non-negative integers.
 */
function cachedDecodeConfig(
  idx: number,
  cache: Record<string, string>[],
  configKeys: string[],
  configValues: string[][],
): Record<string, string> {
  return cache[idx] ?? (cache[idx] = decodeConfig(idx, configKeys, configValues))
}

function configSpaceSize(configValues: string[][]): number {
  return configValues.reduce((total, values) => total * values.length, 1)
}

function assertValidBenchIdx(benchIdx: number, benchCount: number): void {
  if (!Number.isInteger(benchIdx) || benchIdx < 0 || benchIdx >= benchCount) {
    throw new Error(`Invalid bench_idx '${benchIdx}' for ${benchCount} benchmarks`)
  }
}

function assertValidConfigIdx(configIdx: number, configCount: number): void {
  if (!Number.isInteger(configIdx) || configIdx < 0 || configIdx >= configCount) {
    throw new Error(`Invalid config_idx '${configIdx}' for ${configCount} configs`)
  }
}

export function decodeResults(index: HostIndex, data: IndexedResults): CompactResult[] {
  const { keys, values } = indexConfigTables(index)
  const configCount = configSpaceSize(values)
  const benchNames = index.benchmarks.map((b) => b.name)
  const cache: Record<string, string>[] = []
  return data.results.map((row) => {
    assertValidBenchIdx(row[0], benchNames.length)
    assertValidConfigIdx(row[1], configCount)
    return {
      bench: benchNames[row[0]],
      config: cachedDecodeConfig(row[1], cache, keys, values),
      mean_ns: row[2],
      ci95_half_ns: row[3],
    }
  })
}

/** Decode all rows for one benchmark name from a host's compact results payload. */
export function decodeResultsForBenchmark(index: HostIndex, data: IndexedResults, bench: string): CompactResult[] {
  const benchIdx = index.benchmarks.findIndex((b) => b.name === bench)
  if (benchIdx === -1) return []
  const { keys, values } = indexConfigTables(index)
  const configCount = configSpaceSize(values)
  const benchCount = index.benchmarks.length
  for (const row of data.results) {
    assertValidBenchIdx(row[0], benchCount)
    assertValidConfigIdx(row[1], configCount)
  }

  const cache: Record<string, string>[] = []
  return data.results
    .filter((row: ResultRow) => row[0] === benchIdx)
    .map((row: ResultRow) => ({
      bench,
      config: cachedDecodeConfig(row[1], cache, keys, values),
      mean_ns: row[2],
      ci95_half_ns: row[3],
    }))
}

/** Decode pre-computed latest timeline rows from host index metadata. */
export function decodeLatestResults(index: HostIndex): CompactResult[] | null {
  const rows = index.latest_results
  if (!rows) return null
  const { keys, values } = indexConfigTables(index)
  const configCount = configSpaceSize(values)
  const benchNames = index.benchmarks.map((b) => b.name)
  const cache: Record<string, string>[] = []
  return rows.map((row) => {
    assertValidBenchIdx(row[0], benchNames.length)
    assertValidConfigIdx(row[1], configCount)
    return {
      bench: benchNames[row[0]],
      config: cachedDecodeConfig(row[1], cache, keys, values),
      mean_ns: row[2],
      ci95_half_ns: row[3],
    }
  })
}

/** Decode one benchmark history payload and keep only rows for a target config. */
export function decodeHistory(index: HostIndex, data: IndexedHistory, config: Record<string, string>): HistorySeries[] {
  const { keys, values } = indexConfigTables(index)
  const configCount = configSpaceSize(values)
  const configIdx = encodeConfig(config, keys, values)
  if (configIdx === -1) {
    return []
  }
  const cache: Record<string, string>[] = []
  for (const row of data.series) {
    assertValidConfigIdx(row[0], configCount)
  }
  return data.series
    .filter((row) => row[0] === configIdx)
    .map((row) => ({
      config: cachedDecodeConfig(row[0], cache, keys, values),
      timestamp: row[1],
      median_run_mean_ns: row[2],
      median_run_ci95_half_ns: row[3],
      run_count: row[4],
    }))
}
