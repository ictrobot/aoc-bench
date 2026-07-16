import { useEffect, useId, useMemo, useRef } from "react"
import { ExternalLink } from "lucide-react"
import { useUrlHostBenchmark, useUrlFilters, useUrlParam } from "@/hooks/use-url-state.tsx"
import { useLatestResults } from "@/hooks/queries.ts"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { BenchmarkConfigTable, type BenchmarkConfigTableRow } from "@/components/benchmarks/BenchmarkConfigTable.tsx"
import { ConfigFilter } from "@/components/config/ConfigFilter.tsx"
import { DashboardInsights } from "@/components/benchmarks/DashboardInsights.tsx"
import { compareConfigsByOrder } from "@/lib/config-order.ts"
import { formatDurationNs, shortenValue } from "@/lib/format.ts"
import type { CompactResult } from "@/lib/types.ts"
import { withQuery, configValueUrl } from "@/lib/routes.ts"
import { MarkdownLinks } from "@/components/ui/markdown-links.tsx"

export function Dashboard() {
  const { host, hostIndex } = useUrlHostBenchmark()
  const { benchmarks, config_keys: configKeys, description, timeline_key: timelineKey } = hostIndex
  const { filters, setFilter } = useUrlFilters()
  const [benchFilter, setBenchFilter] = useUrlParam("benchFilter")
  const { data: latestResults, error: decodeError } = useLatestResults(host)

  const benchRegex = useMemo(() => {
    if (!benchFilter) return null
    try {
      return new RegExp(benchFilter, "i")
    } catch {
      return null
    }
  }, [benchFilter])
  const benchFilterInvalid = benchFilter !== "" && benchRegex === null

  const hasLatestResults = (latestResults?.length ?? 0) > 0
  const showFastestColumns = timelineKey !== null && hasLatestResults
  const currentTimelineValue = showFastestColumns && timelineKey ? (configKeys[timelineKey]?.values.at(-1) ?? "") : ""
  const timelineLinkTemplate = timelineKey ? configKeys[timelineKey]?.link : undefined
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

  const visibleBenchmarks = useMemo(
    () => (benchRegex ? benchmarks.filter((b) => benchRegex.test(b.name)) : benchmarks),
    [benchmarks, benchRegex],
  )

  const insightEntries = useMemo(
    () =>
      [...fastestByBenchmark.entries()]
        .filter(([name]) => !benchRegex || benchRegex.test(name))
        .map(([name, result]) => ({
          name,
          mean_ns: result.mean_ns,
          href: withQuery("/benchmark", { host, bench: name }),
        })),
    [fastestByBenchmark, host, benchRegex],
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
          <DashboardFilters
            timelineKey={timelineKey}
            currentTimelineValue={currentTimelineValue}
            timelineLinkTemplate={timelineLinkTemplate}
            configKeys={configKeys}
            filterKeys={filterKeys}
            filters={effectiveFilters}
            setFilter={setFilter}
            benchFilter={benchFilter}
            benchFilterInvalid={benchFilterInvalid}
            setBenchFilter={setBenchFilter}
          />
          <Card>
            <CardHeader>
              <CardTitle>Benchmarks</CardTitle>
            </CardHeader>
            <CardContent>
              <DashboardBenchmarkTable
                key={host}
                benchmarks={visibleBenchmarks}
                host={host}
                configKeys={configKeys}
                configColumnKeys={visibleConfigColumnKeys}
                fastestByBenchmark={fastestByBenchmark}
                showFastestColumns={showFastestColumns}
                hasActiveFilters={hasActiveFilters}
              />
            </CardContent>
          </Card>
          {insightEntries.length > 0 && <DashboardInsights entries={insightEntries} />}
        </>
      )}
    </div>
  )
}

function DashboardFilters({
  timelineKey,
  currentTimelineValue,
  timelineLinkTemplate,
  configKeys,
  filterKeys,
  filters,
  setFilter,
  benchFilter,
  benchFilterInvalid,
  setBenchFilter,
}: {
  timelineKey: string | null
  currentTimelineValue: string
  timelineLinkTemplate?: string
  configKeys: Record<string, { values: string[] }>
  filterKeys: string[]
  filters: Record<string, string>
  setFilter: (key: string, value: string) => void
  benchFilter: string
  benchFilterInvalid: boolean
  setBenchFilter: (value: string) => void
}) {
  const benchFilterId = useId()
  // The input is uncontrolled: react-router applies setSearchParams in a transition
  // after the change event, and re-controlling the input from that late update resets
  // the cursor to the end when editing mid-value. Sync the DOM value from the URL only
  // while the input is not focused, so external updates (e.g. back/forward navigation)
  // still show up without clobbering in-progress typing.
  const benchFilterRef = useRef<HTMLInputElement>(null)
  useEffect(() => {
    const el = benchFilterRef.current
    if (el && document.activeElement !== el && el.value !== benchFilter) {
      el.value = benchFilter
    }
  }, [benchFilter])

  const displayTimelineValue = shortenValue(currentTimelineValue)
  const timelineValueTitle = displayTimelineValue !== currentTimelineValue ? currentTimelineValue : undefined

  return (
    <div className="flex flex-wrap items-end gap-3">
      <div className="flex items-center gap-2">
        <label htmlFor={benchFilterId} className="text-sm text-muted-foreground whitespace-nowrap">
          Benchmark:
        </label>
        <input
          ref={benchFilterRef}
          id={benchFilterId}
          type="text"
          defaultValue={benchFilter}
          onChange={(e) => setBenchFilter(e.target.value)}
          placeholder="Filter (regex)"
          spellCheck={false}
          aria-invalid={benchFilterInvalid || undefined}
          title={benchFilterInvalid ? "Invalid regular expression" : undefined}
          className={`h-9 w-[160px] rounded-md border bg-transparent px-3 py-2 font-mono text-sm shadow-xs placeholder:font-sans placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring ${benchFilterInvalid ? "border-destructive" : "border-input"}`}
        />
      </div>
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
          {timelineLinkTemplate ? (
            <a
              href={configValueUrl(timelineLinkTemplate, currentTimelineValue)}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 font-mono text-foreground hover:underline"
              title={timelineValueTitle}
            >
              {displayTimelineValue}
              <ExternalLink className="size-3.5 text-muted-foreground" />
            </a>
          ) : (
            <span className="font-mono text-foreground" title={timelineValueTitle}>
              {displayTimelineValue}
            </span>
          )}
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
