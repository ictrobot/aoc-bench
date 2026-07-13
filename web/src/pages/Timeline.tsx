import { useEffect, useId, useMemo, useState, useTransition } from "react"
import { ExternalLink } from "lucide-react"
import { useUrlHostBenchmark, useUrlParam, useUrlFilters } from "@/hooks/use-url-state.tsx"
import { useBenchmarkResults, useBenchmarkHistory } from "@/hooks/queries.ts"
import { ConfigFilter } from "@/components/config/ConfigFilter.tsx"
import { HistoryChart } from "@/components/charts/HistoryChart.tsx"
import { StepTimelineChart, type TimelineChartGroup } from "@/components/charts/StepTimelineChart.tsx"
import { Card, CardAction, CardContent, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog.tsx"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table.tsx"
import { formatDurationNs, formatCi, shortenValue } from "@/lib/format.ts"
import { Combobox } from "@/components/ui/combobox.tsx"
import { Badge } from "@/components/ui/badge.tsx"
import { Button } from "@/components/ui/button.tsx"
import type { CompactResult, HostIndex } from "@/lib/types.ts"
import { relativeChange } from "@/lib/delta.ts"
import { groupTimelineResults } from "@/lib/timeline-grouping.ts"
import { GroupValueLabel } from "@/components/config/GroupValueLabel.tsx"
import { configValueUrl } from "@/lib/routes.ts"

export function Timeline() {
  const { host, bench, hostIndex, setBench } = useUrlHostBenchmark()

  useEffect(() => {
    document.title = bench ? `${bench} — Timeline — aoc-bench` : "Timeline — aoc-bench"
  }, [bench])

  return (
    <TimelineContent key={`${host}:${bench}`} host={host} bench={bench} hostIndex={hostIndex} setBench={setBench} />
  )
}

interface TimelineContentProps {
  host: string
  bench: string
  hostIndex: HostIndex
  setBench: (bench: string, replace?: boolean) => void
}

function TimelineContent({ host, bench, hostIndex: index, setBench }: TimelineContentProps) {
  const { data: results, isLoading, error } = useBenchmarkResults(host, bench)

  // Determine which keys vary and which are fixed
  const analysis = useMemo(() => {
    if (!results || results.length === 0) return null

    const allKeys = Object.keys(results[0].config)
    const keyValues = new Map<string, Set<string>>()
    for (const key of allKeys) {
      keyValues.set(key, new Set())
    }
    for (const r of results) {
      for (const [k, v] of Object.entries(r.config)) {
        keyValues.get(k)?.add(v)
      }
    }

    const varying = allKeys.filter((k) => (keyValues.get(k)?.size ?? 0) > 1)
    const fixed = allKeys.filter((k) => (keyValues.get(k)?.size ?? 0) <= 1)

    return { allKeys, keyValues, varying, fixed }
  }, [results])

  const [compare, setCompare] = useUrlParam("compare", "", analysis?.varying)
  const { filters: urlFilters, setFilter, clearFilters } = useUrlFilters()

  const [isPending, startTransition] = useTransition()
  const [selected, setSelected] = useState<{ group: TimelineChartGroup; value?: string } | null>(null)
  const compareByControlId = useId()
  const compareByLabelId = `${compareByControlId}-label`

  const defaults = useMemo(() => {
    if (!results || !analysis) {
      return { varyingKey: "", filters: {} as Record<string, string> }
    }
    const defaultVaryingKey =
      index.timeline_key && analysis.varying.includes(index.timeline_key)
        ? index.timeline_key
        : (analysis.varying[0] ?? "")

    // Store a default value for every varying key in canonical order.
    const defaultFilters: Record<string, string> = {}
    for (const key of analysis.allKeys) {
      const inData = analysis.keyValues.get(key) ?? new Set<string>()
      if (inData.size <= 1) continue
      const canonicalOrder = index.config_keys[key]?.values ?? []
      defaultFilters[key] = canonicalOrder.find((v) => inData.has(v)) ?? [...inData][0]
    }

    return { varyingKey: defaultVaryingKey, filters: defaultFilters }
  }, [analysis, index, results])

  const filters = useMemo(() => {
    if (!analysis) return {}
    const merged = { ...defaults.filters, ...urlFilters }
    const sanitized: Record<string, string> = {}
    for (const key of analysis.allKeys) {
      const value = merged[key]
      if (!value) continue
      if (analysis.keyValues.get(key)?.has(value)) {
        sanitized[key] = value
      }
    }
    return sanitized
  }, [analysis, defaults.filters, urlFilters])

  // Auto-select varying key
  const preferredVaryingKey = compare || defaults.varyingKey
  const effectiveVaryingKey = useMemo(() => {
    if (!analysis) return ""
    if (preferredVaryingKey && analysis.varying.includes(preferredVaryingKey)) return preferredVaryingKey
    // After applying filters, re-check which keys still vary
    const filtered = filterResultsByConfig(results ?? [], filters, "")
    const remaining = analysis.varying.filter((k) => {
      const vals = new Set(filtered.map((r) => r.config[k]))
      return vals.size > 1
    })
    if (remaining.length > 0) return remaining[0]
    return analysis.varying[0] ?? ""
  }, [analysis, preferredVaryingKey, filters, results])

  // Filter and sort results
  const chartData = useMemo((): TimelineChartGroup[] => {
    if (!results || !effectiveVaryingKey) return []

    const filtered = filterResultsByConfig(results, filters, effectiveVaryingKey)

    // Get value order from index config_keys if available
    const valueOrder = index.config_keys[effectiveVaryingKey]?.values ?? []

    const sorted = filtered.sort((a, b) => {
      const ai = valueOrder.indexOf(a.config[effectiveVaryingKey])
      const bi = valueOrder.indexOf(b.config[effectiveVaryingKey])
      if (ai !== -1 && bi !== -1) return ai - bi
      return (a.config[effectiveVaryingKey] ?? "").localeCompare(b.config[effectiveVaryingKey] ?? "")
    })

    const grouped = groupTimelineResults(
      sorted,
      effectiveVaryingKey,
      valueOrder,
      index.config_keys[effectiveVaryingKey]?.annotations ?? {},
    )
    return grouped.map((group, i) => {
      const prev = i > 0 ? grouped[i - 1] : null
      let color = "var(--color-chart-1)" // no significant change
      let delta: number | null = null
      if (prev) {
        const relChange = relativeChange(group.mean_ns, prev.mean_ns)
        delta = relChange
        if (relChange !== null && Math.abs(relChange) > 0.1) {
          color = relChange > 0 ? "var(--color-destructive)" : "var(--color-improvement)"
        }
      }
      return {
        ...group,
        color,
        delta,
      }
    })
  }, [results, filters, effectiveVaryingKey, index])

  // Top 10 changes >10% by magnitude — markers + sparse labelling in overview mode
  const significantGroupIndices = useMemo((): Set<number> => {
    return new Set(
      chartData
        .map((d, i) => ({ i, abs: d.delta !== null ? Math.abs(d.delta) : 0 }))
        .filter((d) => d.abs > 0.1)
        .sort((a, b) => b.abs - a.abs)
        .slice(0, 10)
        .map((d) => d.i),
    )
  }, [chartData])

  const totalCases = useMemo(() => chartData.reduce((sum, group) => sum + group.caseCount, 0), [chartData])
  const hasAnnotations = chartData.some((d) => d.annotations.length > 0)
  const linkTemplate = index.config_keys[effectiveVaryingKey]?.link

  const [detailMode, setDetailMode] = useState(false)
  const MIN_DETAIL_BAR_PX = 20

  // Benchmark selector
  const benchmarks = index.benchmarks

  function onBenchChange(b: string) {
    startTransition(() => {
      setBench(b)
      setSelected(null)
    })
  }

  const filterKeys = analysis?.allKeys.filter((k) => k !== effectiveVaryingKey) ?? []
  const hasActiveFilterOverrides = Object.values(urlFilters).some(Boolean) || compare !== ""

  function resetViewFilters() {
    clearFilters()
    setCompare("")
  }

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-bold">Timeline</h1>
      <div className="border rounded-lg px-4 py-3 space-y-3">
        <div className="flex flex-wrap items-center gap-3">
          <BenchmarkSelector benchmarks={benchmarks} value={bench} onChange={onBenchChange} />
          {analysis && analysis.varying.length > 1 && (
            <div className="flex items-center gap-2">
              <label
                id={compareByLabelId}
                htmlFor={compareByControlId}
                className="text-sm text-muted-foreground whitespace-nowrap"
              >
                Compare by:
              </label>
              <Combobox
                id={compareByControlId}
                ariaLabelledBy={compareByLabelId}
                value={effectiveVaryingKey}
                onChange={setCompare}
                options={analysis.varying.map((k) => ({ value: k, label: k }))}
                className="w-[140px]"
              />
            </div>
          )}
        </div>
        {filterKeys.some((key) => (analysis?.keyValues.get(key)?.size ?? 0) > 1) && (
          <>
            <div className="border-t" />
            <div className="flex flex-wrap items-center gap-3">
              {filterKeys.map((key) => {
                const valuesInData = analysis?.keyValues.get(key) ?? new Set<string>()
                const canonicalValues = index.config_keys[key]?.values ?? []
                const values = canonicalValues.filter((v) => valuesInData.has(v))
                for (const v of valuesInData) {
                  if (!canonicalValues.includes(v)) values.push(v)
                }
                if (values.length <= 1) return null
                return (
                  <ConfigFilter
                    key={key}
                    label={key}
                    values={values}
                    showAll={false}
                    value={filters[key] ?? ""}
                    onChange={(v) => setFilter(key, v)}
                  />
                )
              })}
            </div>
          </>
        )}
      </div>

      <div className={`space-y-4 ${isPending ? "opacity-50 pointer-events-none" : ""}`}>
        {isLoading && <div className="text-muted-foreground">Loading...</div>}
        {error && <div className="text-destructive">Error: {error.message}</div>}
        {!isLoading && !error && analysis && chartData.length === 0 && (
          <Card>
            <CardContent className="flex flex-col gap-3 py-6 sm:flex-row sm:items-center sm:justify-between">
              <div className="text-sm text-muted-foreground">No data matches the current filter selection.</div>
              {hasActiveFilterOverrides && (
                <Button type="button" variant="outline" size="sm" onClick={resetViewFilters}>
                  Clear filters
                </Button>
              )}
            </CardContent>
          </Card>
        )}
        {chartData.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Performance by {effectiveVaryingKey}</CardTitle>
              <CardAction>
                <div className="flex rounded-md border text-sm overflow-hidden">
                  <button
                    type="button"
                    aria-pressed={!detailMode}
                    onClick={() => setDetailMode(false)}
                    className={`px-3 py-1 transition-colors ${!detailMode ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground"}`}
                  >
                    Overview
                  </button>
                  <button
                    type="button"
                    aria-pressed={detailMode}
                    onClick={() => setDetailMode(true)}
                    className={`px-3 py-1 border-l transition-colors ${detailMode ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground"}`}
                  >
                    Detail
                  </button>
                </div>
              </CardAction>
            </CardHeader>
            <CardContent>
              {detailMode ? (
                <div className="overflow-x-auto">
                  <div style={{ minWidth: Math.max(600, totalCases * MIN_DETAIL_BAR_PX) }}>
                    <StepTimelineChart
                      key={bench}
                      groups={chartData}
                      varyingKey={effectiveVaryingKey}
                      significantGroupIndices={significantGroupIndices}
                      xLabels="all"
                      onGroupClick={(group, value) => setSelected({ group, value })}
                    />
                  </div>
                </div>
              ) : (
                <StepTimelineChart
                  key={bench}
                  groups={chartData}
                  varyingKey={effectiveVaryingKey}
                  significantGroupIndices={significantGroupIndices}
                  xLabels="sparse"
                  onGroupClick={(group, value) => setSelected({ group, value })}
                />
              )}
            </CardContent>
          </Card>
        )}

        {chartData.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Performance breakdown</CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    {hasAnnotations && <TableHead className="w-0" />}
                    <TableHead>{effectiveVaryingKey}</TableHead>
                    <TableHead className="text-right">Mean</TableHead>
                    <TableHead className="text-right">CI</TableHead>
                    <TableHead className="text-right">Delta</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {chartData.map((d, i) => (
                    <TableRow key={i}>
                      {hasAnnotations && (
                        <TableCell className="whitespace-nowrap">
                          {d.annotations.map((annotation) => (
                            <Badge key={annotation.value} variant="secondary" className="font-sans text-xs">
                              {annotation.label}
                            </Badge>
                          ))}
                        </TableCell>
                      )}
                      <TableCell className="text-sm">
                        {linkTemplate && (
                          <a
                            href={configValueUrl(linkTemplate, d.startValue)}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="mr-1.5 inline-flex align-middle text-muted-foreground hover:text-foreground"
                            title={`Open link for ${shortenValue(d.startValue)}`}
                            aria-label={`Open link for ${shortenValue(d.startValue)}`}
                          >
                            <ExternalLink className="size-3.5" />
                          </a>
                        )}
                        <GroupValueLabel group={d} onSelectValue={(value) => setSelected({ group: d, value })} />
                        {d.caseCount > 1 && (
                          <Badge
                            variant="outline"
                            className="ml-2 font-sans text-xs text-muted-foreground"
                            title={`These ${d.caseCount} cases produced identical binaries, so one benchmark result applies to all of them`}
                          >
                            {d.caseCount} identical
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-right">{formatDurationNs(d.mean_ns)}</TableCell>
                      <TableCell className="text-right text-muted-foreground">{formatCi(d.ci95_half_ns)}</TableCell>
                      <TableCell className="text-right">
                        {d.delta !== null ? (
                          <span
                            className={d.delta > 0.1 ? "text-destructive" : d.delta < -0.1 ? "text-improvement" : ""}
                          >
                            {d.delta > 0 ? "+" : ""}
                            {(d.delta * 100).toFixed(2)}%
                          </span>
                        ) : (
                          "\u2014"
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        )}

        <Dialog
          open={selected !== null}
          onOpenChange={(open) => {
            if (!open) setSelected(null)
          }}
        >
          <DialogContent className="max-w-4xl">
            {selected && (
              <DrillDown
                host={host}
                bench={bench}
                configs={selected.group.configs}
                varyingKey={effectiveVaryingKey}
                initialValue={selected.value}
                linkTemplate={linkTemplate}
              />
            )}
          </DialogContent>
        </Dialog>
      </div>
    </div>
  )
}

function BenchmarkSelector({
  benchmarks,
  value,
  onChange,
}: {
  benchmarks: { name: string; result_count: number }[]
  value: string
  onChange: (v: string) => void
}) {
  return (
    <Combobox
      ariaLabel="Select benchmark"
      value={value}
      onChange={onChange}
      options={benchmarks.map((b) => ({ value: b.name, label: b.name }))}
      placeholder="Select benchmark"
      searchPlaceholder="Search benchmarks..."
      className="w-[240px]"
    />
  )
}

export function DrillDown({
  host,
  bench,
  configs,
  varyingKey,
  initialValue,
  linkTemplate,
}: {
  host: string
  bench: string
  configs: Record<string, string>[]
  varyingKey: string
  initialValue?: string
  linkTemplate?: string
}) {
  const selectorId = useId()
  const [selectedValue, setSelectedValue] = useState(initialValue ?? configs.at(-1)?.[varyingKey] ?? "")
  const config = configs.find((candidate) => candidate[varyingKey] === selectedValue) ?? configs.at(-1)!
  const { data, isLoading, error } = useBenchmarkHistory(host, bench, config)

  const configLabel = Object.entries(config)
    .map(([k, v]) => `${k}=${shortenValue(v)}`)
    .join(", ")

  return (
    <>
      <DialogHeader>
        <DialogTitle>History: {configLabel}</DialogTitle>
        {linkTemplate && config[varyingKey] && (
          <a
            href={configValueUrl(linkTemplate, config[varyingKey])}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex w-fit items-center gap-1 text-sm text-muted-foreground hover:text-foreground hover:underline"
          >
            <span className="font-mono">{shortenValue(config[varyingKey])}</span>
            <ExternalLink className="size-3.5" />
          </a>
        )}
      </DialogHeader>
      {configs.length > 1 && (
        <div className="flex items-center gap-2">
          <label id={`${selectorId}-label`} htmlFor={selectorId} className="text-sm text-muted-foreground">
            {varyingKey}:
          </label>
          <Combobox
            id={selectorId}
            ariaLabelledBy={`${selectorId}-label`}
            value={config[varyingKey]}
            onChange={setSelectedValue}
            options={configs.map((candidate) => ({
              value: candidate[varyingKey],
              label: shortenValue(candidate[varyingKey]),
            }))}
            searchPlaceholder={`Search ${varyingKey}...`}
            className="w-[240px]"
          />
        </div>
      )}
      {isLoading && <div className="text-muted-foreground py-8 text-center">Loading history...</div>}
      {error && <div className="text-destructive py-8 text-center">Error loading history: {error.message}</div>}
      {!error &&
        data &&
        (data.length > 0 ? (
          <div className="overflow-y-auto max-h-[70vh]">
            <HistoryChart series={data} />
          </div>
        ) : (
          <div className="text-muted-foreground py-8 text-center">No history found.</div>
        ))}
    </>
  )
}

/** Keep rows that match all selected fixed-dimension filters. */
function filterResultsByConfig(
  results: CompactResult[],
  filters: Record<string, string>,
  varyingKey: string,
): CompactResult[] {
  return results.filter((r) => Object.entries(filters).every(([k, v]) => !v || k === varyingKey || r.config[k] === v))
}
