export interface DashboardInsightEntry {
  name: string
  mean_ns: number
  href: string
}

export interface DashboardInsightSlice {
  name: string
  mean_ns: number
  share: number
  href?: string
  benchmarkCount: number
}

export interface DashboardInsightsData {
  totalNs: number
  benchmarkCount: number
  fastest: DashboardInsightEntry[]
  slowest: DashboardInsightEntry[]
  slices: DashboardInsightSlice[]
}

const DEFAULT_LEADERBOARD_LIMIT = 10
const DEFAULT_SLICE_LIMIT = 10

function compareByMeanAsc(a: DashboardInsightEntry, b: DashboardInsightEntry) {
  return a.mean_ns - b.mean_ns || a.name.localeCompare(b.name)
}

function compareByMeanDesc(a: DashboardInsightEntry, b: DashboardInsightEntry) {
  return b.mean_ns - a.mean_ns || a.name.localeCompare(b.name)
}

export function buildDashboardInsightsData(
  entries: DashboardInsightEntry[],
  {
    leaderboardLimit = DEFAULT_LEADERBOARD_LIMIT,
    sliceLimit = DEFAULT_SLICE_LIMIT,
  }: {
    leaderboardLimit?: number
    sliceLimit?: number
  } = {},
): DashboardInsightsData {
  const totalNs = entries.reduce((sum, entry) => sum + entry.mean_ns, 0)
  const fastest = [...entries].sort(compareByMeanAsc).slice(0, leaderboardLimit)
  const slowest = [...entries].sort(compareByMeanDesc).slice(0, leaderboardLimit)

  const sortedForSlices = [...entries].sort(compareByMeanDesc)
  const slices: DashboardInsightSlice[] = sortedForSlices.slice(0, sliceLimit).map((entry) => ({
    name: entry.name,
    mean_ns: entry.mean_ns,
    share: totalNs === 0 ? 0 : entry.mean_ns / totalNs,
    href: entry.href,
    benchmarkCount: 1,
  }))

  const remainder = sortedForSlices.slice(sliceLimit)
  if (remainder.length > 0) {
    const otherNs = remainder.reduce((sum, entry) => sum + entry.mean_ns, 0)
    slices.push({
      name: "Other",
      mean_ns: otherNs,
      share: totalNs === 0 ? 0 : otherNs / totalNs,
      benchmarkCount: remainder.length,
    })
  }

  return {
    totalNs,
    benchmarkCount: entries.length,
    fastest,
    slowest,
    slices,
  }
}
