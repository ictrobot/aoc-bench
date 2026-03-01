import { useEffect, useMemo, useState } from "react"
import { Link, useSearchParams } from "react-router-dom"
import { useHostIndex, useLatestResults } from "@/hooks/queries.ts"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table.tsx"
import { ConfigFilter } from "@/components/config/ConfigFilter.tsx"
import { BenchmarkLeaderboard } from "@/components/benchmarks/BenchmarkLeaderboard.tsx"
import { formatDurationNs } from "@/lib/format.ts"
import type { CompactResult } from "@/lib/types.ts"
import { withQuery } from "@/lib/routes.ts"

export function Dashboard() {
  const [searchParams] = useSearchParams()
  const host = searchParams.get("host") ?? ""
  const { data, isLoading, error } = useHostIndex(host)
  const { data: latestResults, error: decodeError } = useLatestResults(host)
  const latestResultsWithData = latestResults && latestResults.length > 0 ? latestResults : null

  useEffect(() => {
    document.title = `${host} — aoc-bench`
  }, [host])

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">{host}</h1>
      {isLoading && <div className="text-muted-foreground">Loading...</div>}
      {error && <div className="text-destructive">Error: {error.message}</div>}
      {decodeError && <div className="text-destructive">Error: {decodeError.message}</div>}
      {data && !decodeError && <StatCards benchmarks={data.benchmarks} configKeys={data.config_keys} />}
      {data && !decodeError && (
        <BenchmarkTableWithLatest
          key={host}
          benchmarks={data.benchmarks}
          host={host}
          configKeys={data.config_keys}
          timelineKey={data.timeline_key}
          latestResults={latestResultsWithData}
        />
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

function BenchmarkTableWithLatest({
  benchmarks,
  host,
  configKeys,
  timelineKey,
  latestResults,
}: {
  benchmarks: { name: string; result_count: number }[]
  host: string
  configKeys: Record<string, { values: string[] }>
  timelineKey: string | null
  latestResults: CompactResult[] | null
}) {
  // Filter keys: all config keys in results except timeline_key
  const filterKeys = useMemo(() => {
    if (!latestResults || latestResults.length === 0) return []
    const keys = new Set<string>()
    for (const r of latestResults) {
      for (const k of Object.keys(r.config)) keys.add(k)
    }
    if (timelineKey) keys.delete(timelineKey)
    return [...keys].sort()
  }, [latestResults, timelineKey])

  const [filters, setFilters] = useState<Record<string, string>>({})
  const [sortBy, setSortBy] = useState<"index" | "name" | "time" | "results">("index")
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc")

  // Compute aggregated times: for each bench, min mean_ns across filtered results
  const aggregatedTimes = useMemo(() => {
    if (!latestResults) return new Map<string, number>()
    const filtered = latestResults.filter((r) => Object.entries(filters).every(([k, v]) => !v || r.config[k] === v))
    const map = new Map<string, number>()
    for (const r of filtered) {
      const cur = map.get(r.bench)
      if (cur === undefined || r.mean_ns < cur) map.set(r.bench, r.mean_ns)
    }
    return map
  }, [latestResults, filters])

  // Table rows: all benchmarks, sorted
  const tableRows = useMemo(() => {
    const rows = benchmarks.map((b) => ({
      name: b.name,
      result_count: b.result_count,
      mean_ns: aggregatedTimes.get(b.name) ?? null,
    }))

    if (sortBy === "index") return rows

    return [...rows].sort((a, b) => {
      let cmp
      if (sortBy === "name") {
        cmp = a.name.localeCompare(b.name)
      } else if (sortBy === "time") {
        // Sort by time: null times go last
        if (a.mean_ns === null && b.mean_ns === null) cmp = 0
        else if (a.mean_ns === null) cmp = 1
        else if (b.mean_ns === null) cmp = -1
        else cmp = a.mean_ns - b.mean_ns
      } else {
        cmp = a.result_count - b.result_count
      }
      return sortDir === "asc" ? cmp : -cmp
    })
  }, [benchmarks, aggregatedTimes, sortBy, sortDir])

  function toggleSort(col: "name" | "time" | "results") {
    if (sortBy === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortBy(col)
      setSortDir("asc")
    }
  }

  function sortIndicator(col: "name" | "time" | "results") {
    if (sortBy !== col) return <span className="text-muted-foreground/40 ml-1">↕</span>
    return <span className="ml-1">{sortDir === "asc" ? "↑" : "↓"}</span>
  }

  function sortAria(col: "name" | "time" | "results"): "ascending" | "descending" | "none" {
    if (sortBy !== col) return "none"
    return sortDir === "asc" ? "ascending" : "descending"
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Benchmarks</CardTitle>
        </CardHeader>
        <CardContent>
          {filterKeys.length > 0 && (
            <div className="flex flex-wrap gap-3 mb-4">
              {filterKeys.map((key) => {
                const values = configKeys[key]?.values ?? []
                return (
                  <ConfigFilter
                    key={key}
                    label={key}
                    values={values}
                    value={filters[key] ?? ""}
                    onChange={(v) => setFilters((prev) => ({ ...prev, [key]: v }))}
                  />
                )
              })}
            </div>
          )}
          <div className="max-h-[500px] overflow-y-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead aria-sort={sortAria("name")}>
                    <button
                      type="button"
                      className="cursor-pointer select-none hover:text-foreground"
                      onClick={() => toggleSort("name")}
                    >
                      Benchmark {sortIndicator("name")}
                    </button>
                  </TableHead>
                  {latestResults && (
                    <TableHead className="text-right" aria-sort={sortAria("time")}>
                      <button
                        type="button"
                        className="w-full cursor-pointer select-none text-right hover:text-foreground"
                        onClick={() => toggleSort("time")}
                      >
                        Time (fastest config) {sortIndicator("time")}
                      </button>
                    </TableHead>
                  )}
                  <TableHead className="text-right" aria-sort={sortAria("results")}>
                    <button
                      type="button"
                      className="w-full cursor-pointer select-none text-right hover:text-foreground"
                      onClick={() => toggleSort("results")}
                    >
                      Results {sortIndicator("results")}
                    </button>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tableRows.map((b) => (
                  <TableRow key={b.name}>
                    <TableCell>
                      <Link
                        to={withQuery("/benchmark", { host, bench: b.name })}
                        className="text-primary hover:underline"
                      >
                        {b.name}
                      </Link>
                    </TableCell>
                    {latestResults && (
                      <TableCell className="text-right font-mono text-sm">
                        {b.mean_ns !== null ? formatDurationNs(b.mean_ns) : "—"}
                      </TableCell>
                    )}
                    <TableCell className="text-right">{b.result_count.toLocaleString()}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      {latestResults && (
        <BenchmarkLeaderboard
          entries={[...aggregatedTimes.entries()].map(([name, mean_ns]) => ({ name, mean_ns }))}
          host={host}
        />
      )}
    </div>
  )
}
