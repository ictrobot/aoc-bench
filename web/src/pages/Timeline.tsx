import React, { useEffect, useId, useMemo, useState, useTransition } from "react"
import { useUrlHostBenchmark, useUrlParam, useUrlFilters } from "@/hooks/use-url-state.tsx"
import {
  ComposedChart,
  Bar,
  ErrorBar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Rectangle,
  ReferenceLine,
  type BarShapeProps,
} from "recharts"
import { useBenchmarkResults, useBenchmarkHistory } from "@/hooks/queries.ts"
import { ConfigFilter } from "@/components/config/ConfigFilter.tsx"
import { HistoryChart } from "@/components/charts/HistoryChart.tsx"
import { Card, CardAction, CardContent, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog.tsx"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table.tsx"
import { formatDurationNs, formatCi, shortenValue } from "@/lib/format.ts"
import { Combobox } from "@/components/ui/combobox.tsx"
import { Badge } from "@/components/ui/badge.tsx"
import { Button } from "@/components/ui/button.tsx"
import type { CompactResult, HostIndex } from "@/lib/types.ts"
import { relativeChange } from "@/lib/delta.ts"
import { expandTimelineGroups, groupTimelineResults, type TimelineResultGroup } from "@/lib/timeline-grouping.ts"

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
  const [selectedGroup, setSelectedGroup] = useState<TimelineResultGroup | null>(null)
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
  const chartData = useMemo(() => {
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
      let color = "oklch(0.60 0.15 250)" // blue — no significant change
      let delta: number | null = null
      if (prev) {
        const relChange = relativeChange(group.mean_ns, prev.mean_ns)
        delta = relChange
        if (relChange !== null && Math.abs(relChange) > 0.1) {
          color =
            relChange > 0
              ? "oklch(0.55 0.22 25)" // red — regression
              : "oklch(0.55 0.20 142)" // green — improvement
        }
      }
      return {
        ...group,
        color,
        delta,
      }
    })
  }, [results, filters, effectiveVaryingKey, index])

  const barChartData = useMemo(
    () => expandTimelineGroups(chartData, effectiveVaryingKey),
    [chartData, effectiveVaryingKey],
  )

  // Top 10 changes >10% by magnitude — used for sparse labelling in overview mode
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

  const significantBarIndices = useMemo((): Set<number> => {
    return new Set(
      barChartData.flatMap((bar, index) =>
        bar.isRangeStart && significantGroupIndices.has(bar.groupIndex) ? [index] : [],
      ),
    )
  }, [barChartData, significantGroupIndices])

  const annotatedItems = useMemo(() => chartData.flatMap((group) => group.annotations), [chartData])

  const [detailMode, setDetailMode] = useState(false)
  const MIN_DETAIL_BAR_PX = 20

  // Benchmark selector
  const benchmarks = index.benchmarks

  function onBenchChange(b: string) {
    startTransition(() => {
      setBench(b)
      setSelectedGroup(null)
    })
  }

  const filterKeys = analysis?.allKeys.filter((k) => k !== effectiveVaryingKey) ?? []
  const hasActiveFilterOverrides = Object.values(urlFilters).some(Boolean) || compare !== ""

  function resetViewFilters() {
    clearFilters()
    setCompare("")
  }

  function openChartConfigAtIndex(barIndex: number | undefined) {
    if (barIndex === undefined || barIndex < 0) return
    const bar = barChartData[barIndex]
    const d = bar && chartData[bar.groupIndex]
    if (d) setSelectedGroup(d)
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
                  <div style={{ minWidth: Math.max(600, barChartData.length * MIN_DETAIL_BAR_PX) }}>
                    <ResponsiveContainer key={bench} width="100%" height={400}>
                      <ComposedChart
                        data={barChartData}
                        barCategoryGap={0}
                        margin={{ top: 5, right: 20, bottom: 0, left: 20 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                        <XAxis
                          dataKey="axisValue"
                          angle={-45}
                          textAnchor="end"
                          interval={0}
                          tickFormatter={(value: string, index: number) =>
                            barChartData[index]?.isRangeStart ? shortenValue(value) : ""
                          }
                          tick={{ fontSize: 11 }}
                          height={100}
                        />
                        <YAxis tickFormatter={(v: number) => formatDurationNs(v)} tick={{ fontSize: 11 }} width={80} />
                        <Tooltip content={<TimelineTooltip />} />
                        <Bar
                          dataKey="mean_ns"
                          isAnimationActive={false}
                          onClick={(_data, index) => openChartConfigAtIndex(index)}
                          cursor="pointer"
                          shape={WideTimelineBar}
                        >
                          <ErrorBar
                            dataKey="errorBarCi95HalfNs"
                            width={4}
                            strokeWidth={1.5}
                            stroke="var(--color-foreground)"
                            direction="y"
                          />
                        </Bar>
                        {annotatedItems.map((d) => (
                          <ReferenceLine
                            key={d.value}
                            x={d.value}
                            stroke="var(--color-foreground)"
                            strokeOpacity={0.8}
                            strokeDasharray="6 3"
                            strokeWidth={2.5}
                          />
                        ))}
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              ) : (
                <ResponsiveContainer key={bench} width="100%" height={400}>
                  <ComposedChart
                    data={barChartData}
                    barCategoryGap={0}
                    margin={{ top: 5, right: 20, bottom: 0, left: 20 }}
                  >
                    {barChartData.length <= 30 && <CartesianGrid strokeDasharray="3 3" className="opacity-30" />}
                    <XAxis
                      dataKey="axisValue"
                      angle={-45}
                      textAnchor="end"
                      interval={0}
                      tick={<SparseTick significantIndices={significantBarIndices} />}
                      height={significantBarIndices.size === 0 ? 5 : 80}
                    />
                    <YAxis tickFormatter={(v: number) => formatDurationNs(v)} tick={{ fontSize: 11 }} width={80} />
                    <Tooltip content={<TimelineTooltip />} />
                    {annotatedItems.map((d) => (
                      <ReferenceLine
                        key={d.value}
                        x={d.value}
                        stroke="var(--color-muted-foreground)"
                        strokeDasharray="3 3"
                        label={{
                          value: d.label,
                          position: "top",
                          fontSize: 10,
                          fill: "currentColor",
                        }}
                      />
                    ))}
                    <Bar
                      dataKey="mean_ns"
                      isAnimationActive={false}
                      onClick={(_data, index) => openChartConfigAtIndex(index)}
                      cursor="pointer"
                      shape={WideTimelineBar}
                    />
                  </ComposedChart>
                </ResponsiveContainer>
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
                    <TableHead>{effectiveVaryingKey}</TableHead>
                    <TableHead className="text-right">Mean</TableHead>
                    <TableHead className="text-right">CI</TableHead>
                    <TableHead className="text-right">Delta</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {chartData.map((d, i) => (
                    <TableRow key={i}>
                      <TableCell className="font-mono text-sm">
                        <button
                          type="button"
                          onClick={() => setSelectedGroup(d)}
                          className="hover:underline"
                          title={d.fullValue}
                          aria-label={`Open history for ${d.fullValue}`}
                        >
                          {shortenValue(d.fullValue)}
                        </button>
                        {d.annotations.map((annotation) => (
                          <Badge key={annotation.value} variant="outline" className="ml-2 font-sans text-xs">
                            {annotation.label}
                          </Badge>
                        ))}
                        {d.caseCount > 1 && (
                          <Badge
                            variant="secondary"
                            className="ml-2 font-sans text-xs"
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
                          <span className={d.delta > 0.1 ? "text-destructive" : d.delta < -0.1 ? "text-green-600" : ""}>
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
          open={selectedGroup !== null}
          onOpenChange={(open) => {
            if (!open) setSelectedGroup(null)
          }}
        >
          <DialogContent className="max-w-4xl">
            {selectedGroup && (
              <DrillDown host={host} bench={bench} configs={selectedGroup.configs} varyingKey={effectiveVaryingKey} />
            )}
          </DialogContent>
        </Dialog>
      </div>
    </div>
  )
}

interface WideTimelineBarPayload {
  isRangeStart: boolean
  caseCount: number
  color: string
}

/** Draw one real rectangle over all of the case-width slots reserved for a merged range. */
function WideTimelineBar(props: BarShapeProps) {
  const payload = props.payload as WideTimelineBarPayload | undefined
  if (!payload?.isRangeStart) return <g />

  return (
    <Rectangle
      {...props}
      width={Math.max(0, props.width * payload.caseCount - 1)}
      fill={payload.color}
      radius={[2, 2, 0, 0]}
    />
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

function TimelineTooltip({
  active,
  payload,
}: {
  active?: boolean
  payload?: Array<{
    payload: {
      fullValue: string
      mean_ns: number
      ci95_half_ns: number
      delta: number | null
      annotations: { value: string; label: string }[]
      caseCount: number
    }
  }>
}) {
  if (!active || !payload?.[0]) return null
  const d = payload[0].payload
  return (
    <div className="rounded-md border bg-background p-3 shadow-md text-sm">
      <div className="font-medium">{d.fullValue}</div>
      {d.annotations.map((annotation) => (
        <div key={annotation.value}>
          {shortenValue(annotation.value)}: {annotation.label}
        </div>
      ))}
      <div>
        Mean: {formatDurationNs(d.mean_ns)}
        <span className="text-muted-foreground"> {formatCi(d.ci95_half_ns)}</span>
      </div>
      {d.delta !== null && (
        <div>
          Delta: {d.delta > 0 ? "+" : ""}
          {(d.delta * 100).toFixed(2)}%
        </div>
      )}
    </div>
  )
}

export function DrillDown({
  host,
  bench,
  configs,
  varyingKey,
}: {
  host: string
  bench: string
  configs: Record<string, string>[]
  varyingKey: string
}) {
  const selectorId = useId()
  const [selectedValue, setSelectedValue] = useState(configs.at(-1)?.[varyingKey] ?? "")
  const config = configs.find((candidate) => candidate[varyingKey] === selectedValue) ?? configs.at(-1)!
  const { data, isLoading, error } = useBenchmarkHistory(host, bench, config)

  const configLabel = Object.entries(config)
    .map(([k, v]) => `${k}=${shortenValue(v)}`)
    .join(", ")

  return (
    <>
      <DialogHeader>
        <DialogTitle>History: {configLabel}</DialogTitle>
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

function SparseTick({
  x = 0,
  y = 0,
  payload,
  index = 0,
  significantIndices,
}: React.SVGProps<SVGGElement> & {
  payload?: { value: string }
  index?: number
  significantIndices: Set<number>
}) {
  if (!significantIndices.has(index)) {
    return <g />
  }
  return (
    <g transform={`translate(${x},${y})`}>
      <text x={0} y={0} dy={4} textAnchor="end" fill="currentColor" fontSize={11} transform="rotate(-45)">
        {payload?.value ? shortenValue(String(payload.value)) : ""}
      </text>
    </g>
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
