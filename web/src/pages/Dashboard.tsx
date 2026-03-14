import { useEffect, useMemo } from "react"
import { useUrlHostBenchmark, useUrlFilters } from "@/hooks/use-url-state.tsx"
import { useLatestResults } from "@/hooks/queries.ts"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { BenchmarkConfigTable, type BenchmarkConfigTableRow } from "@/components/benchmarks/BenchmarkConfigTable.tsx"
import { ConfigFilter } from "@/components/config/ConfigFilter.tsx"
import { BenchmarkLeaderboard } from "@/components/benchmarks/BenchmarkLeaderboard.tsx"
import { compareConfigsByOrder } from "@/lib/config-order.ts"
import { formatDurationNs, shortenValue } from "@/lib/format.ts"
import type { CompactResult } from "@/lib/types.ts"
import { withQuery } from "@/lib/routes.ts"
import { MarkdownLinks } from "@/components/ui/markdown-links.tsx"

export function Dashboard() {
  const { host, hostIndex } = useUrlHostBenchmark()
  const { benchmarks, config_keys: configKeys, description, timeline_key: timelineKey } = hostIndex
  const { filters, setFilter } = useUrlFilters()
  const { data: latestResults, error: decodeError } = useLatestResults(host)

  const hasLatestResults = (latestResults?.length ?? 0) > 0
  const showFastestColumns = timelineKey !== null && hasLatestResults
  const currentTimelineValue = showFastestColumns && timelineKey ? (configKeys[timelineKey]?.values.at(-1) ?? "") : ""
  const dashboardFilters = useMemo(() => {
    if (!timelineKey || !filters[timelineKey]) return filters

    const rest = { ...filters }
    delete rest[timelineKey]
    return rest
  }, [filters, timelineKey])

  const effectiveFilters = showFastestColumns ? dashboardFilters : {}
  const filterKeys = showFastestColumns ? Object.keys(configKeys).filter((key) => key !== timelineKey) : []
  const visibleConfigColumnKeys = filterKeys.filter((key) => !effectiveFilters[key])
  const hasActiveFilters = showFastestColumns && Object.values(dashboardFilters).some(Boolean)

  const fastestByBenchmark = useMemo(() => {
    if (!showFastestColumns) return new Map<string, CompactResult>()
    return selectFastestByBenchmark(latestResults ?? [], dashboardFilters, configKeys)
  }, [showFastestColumns, latestResults, dashboardFilters, configKeys])

  const leaderboardEntries = useMemo(
    () => [...fastestByBenchmark.entries()].map(([name, result]) => ({ name, mean_ns: result.mean_ns })),
    [fastestByBenchmark],
  )

  useEffect(() => {
    document.title = `${host} — aoc-bench`
  }, [host])

  useEffect(() => {
    if (timelineKey && filters[timelineKey]) {
      setFilter(timelineKey, "")
    }
  }, [filters, setFilter, timelineKey])

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">{host}</h1>
      {description && (
        <p className="text-sm text-muted-foreground -mt-4">
          <MarkdownLinks text={description} />
        </p>
      )}
      {decodeError && <div className="text-destructive">Error: {decodeError.message}</div>}
      {!decodeError && (
        <>
          <StatCards benchmarks={benchmarks} configKeys={configKeys} />
          <DashboardFilters
            timelineKey={timelineKey}
            currentTimelineValue={currentTimelineValue}
            configKeys={configKeys}
            filterKeys={filterKeys}
            filters={effectiveFilters}
            setFilter={setFilter}
          />
          <Card>
            <CardHeader>
              <CardTitle>Benchmarks</CardTitle>
            </CardHeader>
            <CardContent>
              <DashboardBenchmarkTable
                key={host}
                benchmarks={benchmarks}
                host={host}
                configKeys={configKeys}
                configColumnKeys={visibleConfigColumnKeys}
                fastestByBenchmark={fastestByBenchmark}
                showFastestColumns={showFastestColumns}
                hasActiveFilters={hasActiveFilters}
              />
            </CardContent>
          </Card>
          {leaderboardEntries.length > 0 && <BenchmarkLeaderboard entries={leaderboardEntries} host={host} />}
        </>
      )}
    </div>
  )
}

function StatCards({
  benchmarks,
  configKeys,
}: {
  benchmarks: { name: string; result_count: number }[]
  configKeys: Record<string, { values: string[] }>
}) {
  const totalResults = benchmarks.reduce((s, b) => s + b.result_count, 0)

  return (
    <div className="flex flex-wrap gap-3">
      <Card className="py-3 min-w-28 flex-1">
        <CardContent className="px-4">
          <div className="text-sm font-medium">Benchmarks</div>
          <div className="text-2xl font-bold">{benchmarks.length}</div>
          <div className="text-xs text-muted-foreground">total</div>
        </CardContent>
      </Card>
      <Card className="py-3 min-w-28 flex-1">
        <CardContent className="px-4">
          <div className="text-sm font-medium">Results</div>
          <div className="text-2xl font-bold">{totalResults.toLocaleString()}</div>
          <div className="text-xs text-muted-foreground">total</div>
        </CardContent>
      </Card>
      <div className="w-px self-stretch bg-border mx-1" />
      {Object.entries(configKeys).map(([key, { values }]) => (
        <Card key={key} className="py-3 min-w-28 flex-1">
          <CardContent className="px-4">
            <div className="text-sm font-medium">{key}</div>
            <div className="text-2xl font-bold">{values.length}</div>
            <div className="text-xs text-muted-foreground">values</div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}

function DashboardFilters({
  timelineKey,
  currentTimelineValue,
  configKeys,
  filterKeys,
  filters,
  setFilter,
}: {
  timelineKey: string | null
  currentTimelineValue: string
  configKeys: Record<string, { values: string[] }>
  filterKeys: string[]
  filters: Record<string, string>
  setFilter: (key: string, value: string) => void
}) {
  if (filterKeys.length === 0 && !currentTimelineValue) return null
  const displayTimelineValue = shortenValue(currentTimelineValue)
  const timelineValueTitle = displayTimelineValue !== currentTimelineValue ? currentTimelineValue : undefined

  return (
    <div className="flex flex-wrap items-end gap-3">
      {filterKeys.map((key) => (
        <ConfigFilter
          key={key}
          label={key}
          values={configKeys[key]?.values ?? []}
          value={filters[key] ?? ""}
          onChange={(v) => setFilter(key, v)}
        />
      ))}
      {timelineKey && currentTimelineValue && (
        <div className="self-center text-sm text-muted-foreground">
          <span>Current {timelineKey}: </span>
          <span className="font-mono text-foreground" title={timelineValueTitle}>
            {displayTimelineValue}
          </span>
        </div>
      )}
    </div>
  )
}

interface DashboardRow extends BenchmarkConfigTableRow {
  result_count: number
  fastest: CompactResult | null
}

function DashboardBenchmarkTable({
  benchmarks,
  host,
  configKeys,
  configColumnKeys,
  fastestByBenchmark,
  showFastestColumns,
  hasActiveFilters,
}: {
  benchmarks: { name: string; result_count: number }[]
  host: string
  configKeys: Record<string, { values: string[] }>
  configColumnKeys: string[]
  fastestByBenchmark: Map<string, CompactResult>
  showFastestColumns: boolean
  hasActiveFilters: boolean
}) {
  const rows = useMemo(
    () =>
      benchmarks
        .map((b) => ({
          key: b.name,
          benchmark: b.name,
          benchmarkHref: withQuery("/benchmark", { host, bench: b.name }),
          config: fastestByBenchmark.get(b.name)?.config ?? {},
          result_count: b.result_count,
          fastest: fastestByBenchmark.get(b.name) ?? null,
        }))
        .filter((row) => !showFastestColumns || !hasActiveFilters || row.fastest !== null),
    [benchmarks, fastestByBenchmark, host, showFastestColumns, hasActiveFilters],
  )

  const metricColumns = useMemo(
    () =>
      showFastestColumns
        ? [
            {
              key: "time",
              header: "Time",
              headerClassName: "text-right",
              cellClassName: "text-right font-mono text-sm",
              compare: (a: DashboardRow, b: DashboardRow) => {
                if (a.fastest === null && b.fastest === null) return 0
                if (a.fastest === null) return 1
                if (b.fastest === null) return -1
                return a.fastest.mean_ns - b.fastest.mean_ns
              },
              render: (row: DashboardRow) => (row.fastest !== null ? formatDurationNs(row.fastest.mean_ns) : "—"),
            },
          ]
        : [
            {
              key: "results",
              header: "Result count",
              headerClassName: "text-right",
              cellClassName: "text-right",
              compare: (a: DashboardRow, b: DashboardRow) => a.result_count - b.result_count,
              render: (row: DashboardRow) => row.result_count.toLocaleString(),
            },
          ],
    [showFastestColumns],
  )

  return (
    <BenchmarkConfigTable
      rows={rows}
      configKeys={configKeys}
      configColumnKeys={showFastestColumns ? configColumnKeys : []}
      configGroupLabel="Fastest config"
      metricColumns={metricColumns}
      containerClassName="max-h-[500px] overflow-y-auto"
    />
  )
}

function selectFastestByBenchmark(
  results: CompactResult[],
  filters: Record<string, string> = {},
  configKeys: Record<string, { values: string[] }>,
): Map<string, CompactResult> {
  const filtered = results.filter((r) => Object.entries(filters).every(([k, v]) => !v || r.config[k] === v))
  const map = new Map<string, CompactResult>()
  for (const r of filtered) {
    const cur = map.get(r.bench)
    if (
      cur === undefined ||
      r.mean_ns < cur.mean_ns ||
      (r.mean_ns === cur.mean_ns && compareConfigsByOrder(r.config, cur.config, configKeys) < 0)
    ) {
      map.set(r.bench, r)
    }
  }
  return map
}
