import { useDeferredValue, useEffect, useId, useMemo } from "react"
import { Link } from "react-router"
import { useUrlHostBenchmark, useUrlParam, useUrlFilters, useSetUrlParams } from "@/hooks/use-url-state.tsx"
import { useCompactResults } from "@/hooks/queries.ts"
import { ConfigFilter } from "@/components/config/ConfigFilter.tsx"
import { Combobox } from "@/components/ui/combobox.tsx"
import { shortenValue } from "@/lib/format.ts"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { Badge } from "@/components/ui/badge.tsx"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table.tsx"
import { formatDurationNs } from "@/lib/format.ts"
import type { CompactResult } from "@/lib/types.ts"
import { withQuery } from "@/lib/routes.ts"
import { buildBenchmarkConfigSignature } from "@/lib/config-signature.ts"
import { relativeChange } from "@/lib/delta.ts"

interface ImpactEntry {
  bench: string
  config: Record<string, string>
  fromMean: number
  toMean: number
  relChange: number
  direction: "regression" | "improvement"
}

const validateThreshold = (v: string) => !isNaN(Number(v)) && Number(v) >= 0 && Number(v) <= 100

export function Impact() {
  const { host } = useUrlHostBenchmark()

  return <ImpactContent key={host} host={host} />
}

function ImpactContent({ host }: { host: string }) {
  const setUrlParams = useSetUrlParams()
  useEffect(() => {
    document.title = "Impact — aoc-bench"
  }, [])

  const { hostIndex: index } = useUrlHostBenchmark()
  const { data: compact, isLoading, error } = useCompactResults(host)

  const configKeys = index.config_keys
  const keyNames = useMemo(() => Object.keys(configKeys), [configKeys])
  const defaultComparisonKey = index.timeline_key || keyNames[0] || ""
  const [comparisonKey] = useUrlParam("compare", defaultComparisonKey, keyNames)
  const setComparisonKey = (v: string) => setUrlParams({ compare: v, from: null, to: null })

  const comparisonValues = useMemo(() => configKeys[comparisonKey]?.values ?? [], [configKeys, comparisonKey])
  const [fromValue] = useUrlParam("from", "", comparisonValues)
  const setFromValue = (v: string) => setUrlParams({ from: v, to: null })
  const [toValue, setToValue] = useUrlParam("to", "", comparisonValues)

  const [thresholdStr, setThresholdStr] = useUrlParam("threshold", "10", validateThreshold)
  const threshold = Number(thresholdStr)
  const deferredThreshold = useDeferredValue(threshold)

  const { filters, setFilter } = useUrlFilters()

  const compareByControlId = useId()
  const compareByLabelId = `${compareByControlId}-label`
  const fromControlId = useId()
  const fromLabelId = `${fromControlId}-label`
  const toControlId = useId()
  const toLabelId = `${toControlId}-label`
  const thresholdControlId = useId()

  // Compute impact
  const impact = useMemo(() => {
    if (!compact || !comparisonKey || !fromValue || !toValue) return null

    const thresholdFrac = deferredThreshold / 100

    // Filter results by non-comparison filters
    const filtered = compact.filter((r) =>
      Object.entries(filters).every(([k, v]) => !v || k === comparisonKey || r.config[k] === v),
    )

    // Group by (bench, config-minus-comparison-key)
    const fromMap = new Map<string, CompactResult>()
    const toMap = new Map<string, CompactResult>()

    for (const r of filtered) {
      if (r.config[comparisonKey] === fromValue) {
        fromMap.set(buildBenchmarkConfigSignature(r.bench, r.config, comparisonKey), r)
      } else if (r.config[comparisonKey] === toValue) {
        toMap.set(buildBenchmarkConfigSignature(r.bench, r.config, comparisonKey), r)
      }
    }

    const regressions: ImpactEntry[] = []
    const improvements: ImpactEntry[] = []
    let unchanged = 0

    for (const [key, toResult] of toMap) {
      const fromResult = fromMap.get(key)
      if (!fromResult) continue

      const relChange = relativeChange(toResult.mean_ns, fromResult.mean_ns)
      if (relChange === null) continue

      // Check CI overlap
      const fromLow = fromResult.mean_ns - fromResult.ci95_half_ns
      const fromHigh = fromResult.mean_ns + fromResult.ci95_half_ns
      const toLow = toResult.mean_ns - toResult.ci95_half_ns
      const toHigh = toResult.mean_ns + toResult.ci95_half_ns
      const overlap = !(fromHigh < toLow || toHigh < fromLow)

      if (overlap || Math.abs(relChange) < thresholdFrac) {
        unchanged++
        continue
      }

      // Strip comparison key from config for display
      const displayConfig = { ...toResult.config }
      delete displayConfig[comparisonKey]

      const entry: ImpactEntry = {
        bench: toResult.bench,
        config: displayConfig,
        fromMean: fromResult.mean_ns,
        toMean: toResult.mean_ns,
        relChange: Math.abs(relChange),
        direction: relChange > 0 ? "regression" : "improvement",
      }

      if (relChange > 0) {
        regressions.push(entry)
      } else {
        improvements.push(entry)
      }
    }

    regressions.sort((a, b) => b.relChange - a.relChange)
    improvements.sort((a, b) => b.relChange - a.relChange)

    return { regressions, improvements, unchanged }
  }, [compact, comparisonKey, fromValue, toValue, filters, deferredThreshold])

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-bold">Impact</h1>

      <div className="border rounded-lg px-4 py-3 space-y-3">
        <div className="flex flex-wrap items-center gap-3">
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
              value={comparisonKey}
              onChange={setComparisonKey}
              options={keyNames.map((k) => ({ value: k, label: k }))}
              placeholder="Select key"
              className="w-[160px]"
            />
          </div>
          {comparisonKey && (
            <>
              <div className="flex items-center gap-2">
                <label id={fromLabelId} htmlFor={fromControlId} className="text-sm text-muted-foreground">
                  From:
                </label>
                <Combobox
                  id={fromControlId}
                  ariaLabelledBy={fromLabelId}
                  value={fromValue}
                  onChange={setFromValue}
                  options={comparisonValues.slice(0, -1).map((v) => {
                    const ann = configKeys[comparisonKey]?.annotations?.[v]
                    return { value: v, label: ann ? `${shortenValue(v)} - ${ann}` : shortenValue(v) }
                  })}
                  placeholder="Select"
                  className="w-[200px]"
                />
              </div>
              <div className="flex items-center gap-2">
                <label id={toLabelId} htmlFor={toControlId} className="text-sm text-muted-foreground">
                  To:
                </label>
                <Combobox
                  id={toControlId}
                  ariaLabelledBy={toLabelId}
                  value={toValue}
                  onChange={(v) => setToValue(v)}
                  options={comparisonValues.slice(fromValue ? comparisonValues.indexOf(fromValue) + 1 : 0).map((v) => {
                    const ann = configKeys[comparisonKey]?.annotations?.[v]
                    return { value: v, label: ann ? `${shortenValue(v)} - ${ann}` : shortenValue(v) }
                  })}
                  placeholder="Select"
                  className="w-[200px]"
                />
              </div>
            </>
          )}
          <div className="flex items-center gap-2">
            <label htmlFor={thresholdControlId} className="text-sm text-muted-foreground whitespace-nowrap">
              Threshold:
            </label>
            <input
              id={thresholdControlId}
              type="number"
              min={0}
              max={100}
              value={threshold}
              onChange={(e) => setThresholdStr(e.target.value)}
              className="w-[70px] rounded-md border px-2 py-1 text-sm bg-transparent"
            />
            <span className="text-sm text-muted-foreground">%</span>
          </div>
        </div>
        {keyNames.filter((k) => k !== comparisonKey && (configKeys[k]?.values.length ?? 0) > 1).length > 0 && (
          <>
            <div className="border-t" />
            <div className="flex flex-wrap items-center gap-3">
              {keyNames
                .filter((k) => k !== comparisonKey)
                .map((key) => {
                  const values = configKeys[key]?.values ?? []
                  if (values.length <= 1) return null
                  return (
                    <ConfigFilter
                      key={key}
                      label={key}
                      values={values}
                      value={filters[key] ?? ""}
                      onChange={(v) => setFilter(key, v)}
                    />
                  )
                })}
            </div>
          </>
        )}
      </div>

      {isLoading && <div className="text-muted-foreground">Loading results...</div>}

      {error && <div className="text-destructive">Error: {error.message}</div>}

      {impact && (
        <div className="space-y-4">
          <div className="flex gap-3">
            <Badge variant="destructive">{impact.regressions.length} regressions</Badge>
            <Badge variant="default">{impact.improvements.length} improvements</Badge>
            <Badge variant="secondary">{impact.unchanged} unchanged</Badge>
          </div>

          {impact.regressions.length > 0 && (
            <ImpactTable title="Regressions" entries={impact.regressions} host={host} variant="destructive" />
          )}

          {impact.improvements.length > 0 && (
            <ImpactTable title="Improvements" entries={impact.improvements} host={host} variant="default" />
          )}
        </div>
      )}
    </div>
  )
}

function ImpactTable({
  title,
  entries,
  host,
  variant,
}: {
  title: string
  entries: ImpactEntry[]
  host: string
  variant: "destructive" | "default"
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          <Badge variant={variant} className="mr-2">
            {entries.length}
          </Badge>
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Benchmark</TableHead>
              <TableHead>Config</TableHead>
              <TableHead className="text-right">From</TableHead>
              <TableHead className="text-right">To</TableHead>
              <TableHead className="text-right">Change</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {entries.map((e, i) => (
              <TableRow key={i}>
                <TableCell>
                  <Link to={withQuery("/benchmark", { host, bench: e.bench })} className="text-primary hover:underline">
                    {e.bench}
                  </Link>
                </TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {Object.entries(e.config)
                    .map(([k, v]) => `${k}=${v}`)
                    .join(", ") || "\u2014"}
                </TableCell>
                <TableCell className="text-right">{formatDurationNs(e.fromMean)}</TableCell>
                <TableCell className="text-right">{formatDurationNs(e.toMean)}</TableCell>
                <TableCell className="text-right">
                  <span className={e.direction === "regression" ? "text-destructive" : "text-green-600"}>
                    {e.direction === "regression" ? "+" : "-"}
                    {(e.relChange * 100).toFixed(2)}%
                  </span>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
