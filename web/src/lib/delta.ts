/** Return relative change `(next - baseline) / baseline`, or null for invalid baselines. */
export function relativeChange(next: number, baseline: number): number | null {
  if (!Number.isFinite(next) || !Number.isFinite(baseline) || baseline <= 0) {
    return null
  }
  return (next - baseline) / baseline
}
