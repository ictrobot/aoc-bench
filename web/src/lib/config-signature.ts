function sortedConfigEntries(config: Record<string, string>, excludedKey: string): [string, string][] {
  return Object.entries(config)
    .filter(([key]) => key !== excludedKey)
    .sort(([a], [b]) => a.localeCompare(b))
}

/** Build a stable key for a config object while ignoring one dimension. */
export function buildConfigSignature(config: Record<string, string>, excludedKey: string): string {
  return JSON.stringify(sortedConfigEntries(config, excludedKey))
}

/** Build a stable benchmark-scoped key for config pairing operations. */
export function buildBenchmarkConfigSignature(
  bench: string,
  config: Record<string, string>,
  excludedKey: string,
): string {
  return JSON.stringify([bench, sortedConfigEntries(config, excludedKey)])
}
