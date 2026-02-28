import type { GlobalIndex, HostIndex } from "@/lib/types.ts"

export const TEST_HOST = "linux-x64"

export function makeHostIndex(overrides: Partial<HostIndex> = {}): HostIndex {
  return {
    last_updated: 1_700_000_000,
    config_keys: {},
    benchmarks: [],
    timeline_key: null,
    results_path: "results.json",
    history_dir: "history",
    ...overrides,
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
    schema_version: 1,
    snapshot_id: snapshotId,
    hosts: { [host]: hostIndex },
  }
}
