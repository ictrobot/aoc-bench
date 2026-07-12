import type { GlobalIndex, HostIndex } from "@/lib/types.ts"

export const TEST_HOST = "linux-x64"

type BenchmarkOverride = Omit<HostIndex["benchmarks"][number], "config_keys"> & { config_keys?: number[] }
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
  }))
  return index
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
    schema_version: 2,
    snapshot_id: snapshotId,
    hosts: { [host]: hostIndex },
  }
}
