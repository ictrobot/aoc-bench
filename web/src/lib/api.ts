import type { QueryClient } from "@tanstack/react-query"
import type {
  CompactResult,
  GlobalIndex,
  HistoryRow,
  HistorySeries,
  HostIndex,
  IndexedHistory,
  IndexedResults,
  ResultRow,
} from "./types.ts"

const BASE = import.meta.env.BASE_URL + "data"
const WEB_SCHEMA_VERSION = 5

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

/** Mirrors the exporter's link validation: an https URL containing `{value}`. */
function isValidLinkTemplate(link: string): boolean {
  return link.startsWith("https://") && link.includes("{value}")
}

/** Load the top-level host index metadata. */
export async function loadIndex(): Promise<GlobalIndex> {
  const index = await fetchJson<GlobalIndex>("index.json")
  if (index.schema_version !== WEB_SCHEMA_VERSION) {
    throw new Error(`Unsupported web export schema ${index.schema_version}; expected ${WEB_SCHEMA_VERSION}`)
  }

  for (const host of Object.values(index.hosts)) {
    for (const key of Object.values(host.config_keys)) {
      if (key.link !== undefined && !isValidLinkTemplate(key.link)) {
        delete key.link
      }
    }
  }

  return index
}

/**
 * Load the indexed results for one host, expanded into rows.
 *
 * Expansion happens once here so the cached raw data is row-shaped and later per-benchmark
 * decodes only filter.
 */
export async function loadResults(index: HostIndex): Promise<ResultRow[]> {
  return resultRows(await fetchJson<IndexedResults>(index.results_path))
}

/** Load one history part for one benchmark on one host (see `historyPart`), expanded into rows. */
export async function loadHistory(index: HostIndex, bench: string, part: number): Promise<HistoryRow[]> {
  return historyRows(await fetchJson<IndexedHistory>(`${index.history_dir}/${bench}.${part}.json`))
}

/**
 * Find the history part file holding one benchmark config.
 *
 * Returns -1 when the benchmark or config is unknown to the index (nothing to fetch), and
 * throws when the index's part table is malformed.
 */
export function historyPart(index: HostIndex, bench: string, config: Record<string, string>): number {
  const benchIdx = index.benchmarks.findIndex((entry) => entry.name === bench)
  if (benchIdx === -1) return -1
  const { keys, values } = benchmarkConfigTables(index, benchIdx)
  const configIdx = encodeConfig(config, keys, values)
  if (configIdx === -1) return -1

  const parts = index.benchmarks[benchIdx].history_parts
  assertValidHistoryParts(parts, bench)
  let part = 0
  while (part + 1 < parts.length && parts[part + 1] <= configIdx) {
    part++
  }
  return part
}

function assertValidHistoryParts(parts: number[], bench: string): void {
  const valid =
    Array.isArray(parts) &&
    parts.length > 0 &&
    parts[0] === 0 &&
    parts.every((start, i) => Number.isInteger(start) && (i === 0 || start > parts[i - 1]))
  if (!valid) {
    throw new Error(`Invalid history_parts for benchmark '${bench}'`)
  }
}

/** Check that every column is an array of the same length and return that length. */
function sameLength(what: string, ...columns: unknown[]): number {
  if (!columns.every(Array.isArray)) throw new Error(`Missing ${what} column`)
  const length = columns[0].length
  if (columns.some((column) => column.length !== length)) throw new Error(`Mismatched ${what} column lengths`)
  return length
}

/** Expand a results payload into rows, undoing the delta encoding of the index columns. */
export function resultRows({ results }: IndexedResults): ResultRow[] {
  const { bench_idx_delta, config_idx_delta, measurement_token, mean_ns, ci95_half_ns, run_count, multimodal } = results
  const length = sameLength(
    "results",
    bench_idx_delta,
    config_idx_delta,
    measurement_token,
    mean_ns,
    ci95_half_ns,
    run_count,
    multimodal,
  )
  const rows: ResultRow[] = new Array<ResultRow>(length)
  let benchIdx = 0
  let configIdx = 0
  for (let i = 0; i < length; i++) {
    benchIdx += bench_idx_delta[i]
    configIdx += config_idx_delta[i]
    rows[i] = [benchIdx, configIdx, measurement_token[i], mean_ns[i], ci95_half_ns[i], run_count[i], multimodal[i]]
  }
  return rows
}

/** Expand a history part payload into rows, undoing the delta encoding of config and timestamp. */
export function historyRows({ series }: IndexedHistory): HistoryRow[] {
  const { config_idx_delta, measurement_token, timestamp_s_delta, mean_ns, ci95_half_ns, run_count, multimodal } =
    series
  const length = sameLength(
    "history",
    config_idx_delta,
    measurement_token,
    timestamp_s_delta,
    mean_ns,
    ci95_half_ns,
    run_count,
    multimodal,
  )
  const rows: HistoryRow[] = new Array<HistoryRow>(length)
  let configIdx = 0
  let timestamp = 0
  for (let i = 0; i < length; i++) {
    configIdx += config_idx_delta[i]
    timestamp += timestamp_s_delta[i]
    rows[i] = [configIdx, measurement_token[i], timestamp, mean_ns[i], ci95_half_ns[i], run_count[i], multimodal[i]]
  }
  return rows
}

function benchmarkConfigTables(index: HostIndex, benchIdx: number): { keys: string[]; values: string[][] } {
  assertValidBenchIdx(benchIdx, index.benchmarks.length)
  const allKeys = Object.keys(index.config_keys).sort()
  const keyIndexes = index.benchmarks[benchIdx].config_keys
  const seen = new Set<number>()
  const keys: string[] = []
  const values: string[][] = []
  for (const keyIndex of keyIndexes) {
    if (!Number.isInteger(keyIndex) || keyIndex < 0 || keyIndex >= allKeys.length || seen.has(keyIndex)) {
      throw new Error(`Invalid config key index '${keyIndex}' for benchmark '${index.benchmarks[benchIdx].name}'`)
    }
    seen.add(keyIndex)
    keys.push(allKeys[keyIndex])
    values.push(index.config_keys[allKeys[keyIndex]].values)
  }
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

export function decodeResults(index: HostIndex, rows: ResultRow[]): CompactResult[] {
  const benchNames = index.benchmarks.map((b) => b.name)
  const caches: Record<string, string>[][] = index.benchmarks.map(() => [])
  return rows.map((row) => {
    assertValidBenchIdx(row[0], benchNames.length)
    const { keys, values } = benchmarkConfigTables(index, row[0])
    const configCount = configSpaceSize(values)
    assertValidConfigIdx(row[1], configCount)
    return {
      bench: benchNames[row[0]],
      config: cachedDecodeConfig(row[1], caches[row[0]], keys, values),
      measurement_token: row[2],
      mean_ns: row[3],
      ci95_half_ns: row[4],
      run_count: row[5],
      multimodal: row[6] !== 0,
    }
  })
}

/** Decode all rows for one benchmark name from a host's compact results payload. */
export function decodeResultsForBenchmark(index: HostIndex, rows: ResultRow[], bench: string): CompactResult[] {
  const benchIdx = index.benchmarks.findIndex((b) => b.name === bench)
  if (benchIdx === -1) return []
  const benchCount = index.benchmarks.length
  for (const row of rows) {
    assertValidBenchIdx(row[0], benchCount)
    const { values } = benchmarkConfigTables(index, row[0])
    assertValidConfigIdx(row[1], configSpaceSize(values))
  }

  const { keys, values } = benchmarkConfigTables(index, benchIdx)
  const cache: Record<string, string>[] = []
  return rows
    .filter((row: ResultRow) => row[0] === benchIdx)
    .map((row: ResultRow) => ({
      bench,
      config: cachedDecodeConfig(row[1], cache, keys, values),
      measurement_token: row[2],
      mean_ns: row[3],
      ci95_half_ns: row[4],
      run_count: row[5],
      multimodal: row[6] !== 0,
    }))
}

/** Decode pre-computed latest timeline rows from host index metadata. */
export function decodeLatestResults(index: HostIndex): CompactResult[] | null {
  const rows = index.latest_results
  if (!rows) return null
  const benchNames = index.benchmarks.map((b) => b.name)
  const caches: Record<string, string>[][] = index.benchmarks.map(() => [])
  return rows.map((row) => {
    assertValidBenchIdx(row[0], benchNames.length)
    const { keys, values } = benchmarkConfigTables(index, row[0])
    const configCount = configSpaceSize(values)
    assertValidConfigIdx(row[1], configCount)
    return {
      bench: benchNames[row[0]],
      config: cachedDecodeConfig(row[1], caches[row[0]], keys, values),
      measurement_token: row[2],
      mean_ns: row[3],
      ci95_half_ns: row[4],
      run_count: row[5],
      multimodal: row[6] !== 0,
    }
  })
}

/** Decode one benchmark history payload and keep only rows for a target config. */
export function decodeHistory(
  index: HostIndex,
  rows: HistoryRow[],
  bench: string,
  config: Record<string, string>,
): HistorySeries[] {
  const benchIdx = index.benchmarks.findIndex((entry) => entry.name === bench)
  if (benchIdx === -1) return []
  const { keys, values } = benchmarkConfigTables(index, benchIdx)
  const configCount = configSpaceSize(values)
  const configIdx = encodeConfig(config, keys, values)
  if (configIdx === -1) {
    return []
  }
  const cache: Record<string, string>[] = []
  for (const row of rows) {
    assertValidConfigIdx(row[0], configCount)
  }
  return rows
    .filter((row) => row[0] === configIdx)
    .map((row) => ({
      config: cachedDecodeConfig(row[0], cache, keys, values),
      measurement_token: row[1],
      timestamp: row[2],
      mean_ns: row[3],
      ci95_half_ns: row[4],
      run_count: row[5],
      multimodal: row[6] !== 0,
    }))
}
