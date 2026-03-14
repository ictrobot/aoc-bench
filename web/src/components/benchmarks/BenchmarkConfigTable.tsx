import { useMemo, useState, type ReactNode } from "react"
import { Link } from "react-router"
import { shortenValue } from "@/lib/format.ts"
import { compareConfigValueByOrder } from "@/lib/config-order.ts"
import { cn } from "@/lib/utils.ts"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table.tsx"

type ConfigKeys = Record<string, { values: string[] }>
type SortKey = "index" | "benchmark" | `config:${string}` | `metric:${string}`

export interface BenchmarkConfigTableRow {
  key: string
  benchmark: string
  benchmarkHref: string
  config: Record<string, string>
}

export interface BenchmarkConfigMetricColumn<Row extends BenchmarkConfigTableRow> {
  key: string
  header: ReactNode
  render: (row: Row) => ReactNode
  compare?: (a: Row, b: Row) => number
  headerClassName?: string
  cellClassName?: string
}

export function BenchmarkConfigTable<Row extends BenchmarkConfigTableRow>({
  rows,
  configKeys,
  configColumnKeys,
  metricColumns,
  configGroupLabel = "Config",
  benchmarkHeaderLabel = "Benchmark",
  containerClassName,
  emptyState,
}: {
  rows: Row[]
  configKeys: ConfigKeys
  configColumnKeys: string[]
  metricColumns: BenchmarkConfigMetricColumn<Row>[]
  configGroupLabel?: ReactNode
  benchmarkHeaderLabel?: ReactNode
  containerClassName?: string
  emptyState?: ReactNode
}) {
  const [sortBy, setSortBy] = useState<SortKey>("index")
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc")
  const metricColumnsByKey = useMemo(
    () => new Map(metricColumns.map((column) => [column.key, column])),
    [metricColumns],
  )
  const isActiveSortAvailable = useMemo(() => {
    if (sortBy === "index" || sortBy === "benchmark") return true
    if (sortBy.startsWith("config:")) {
      return configColumnKeys.includes(sortBy.slice("config:".length))
    }
    return metricColumnsByKey.has(sortBy.slice("metric:".length))
  }, [sortBy, configColumnKeys, metricColumnsByKey])
  const activeSortBy = isActiveSortAvailable ? sortBy : "index"
  const activeSortDir = isActiveSortAvailable ? sortDir : "asc"

  const sortedRows = useMemo(() => {
    if (activeSortBy === "index") return rows

    return [...rows].sort((a, b) => {
      let cmp
      if (activeSortBy === "benchmark") {
        cmp = a.benchmark.localeCompare(b.benchmark)
      } else if (activeSortBy.startsWith("config:")) {
        const key = activeSortBy.slice("config:".length)
        cmp = compareConfigValueByOrder(a.config[key], b.config[key], configKeys[key]?.values ?? [])
      } else {
        const column = metricColumnsByKey.get(activeSortBy.slice("metric:".length))
        cmp = column?.compare ? column.compare(a, b) : 0
      }
      if (cmp === 0) cmp = a.benchmark.localeCompare(b.benchmark)
      return activeSortDir === "asc" ? cmp : -cmp
    })
  }, [rows, activeSortBy, activeSortDir, configKeys, metricColumnsByKey])

  function toggleSort(col: SortKey) {
    if (sortBy === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortBy(col)
      setSortDir("asc")
    }
  }

  function sortIndicator(col: SortKey) {
    if (activeSortBy !== col) return <span className="text-muted-foreground/40 ml-1">↕</span>
    return <span className="ml-1">{activeSortDir === "asc" ? "↑" : "↓"}</span>
  }

  function sortAria(col: SortKey): "ascending" | "descending" | "none" {
    if (activeSortBy !== col) return "none"
    return activeSortDir === "asc" ? "ascending" : "descending"
  }

  return (
    <div className={cn(containerClassName)}>
      <Table>
        <TableHeader>
          {configColumnKeys.length > 0 ? (
            <>
              <TableRow>
                <TableHead rowSpan={2} className="align-middle" aria-sort={sortAria("benchmark")}>
                  <button
                    type="button"
                    className="cursor-pointer select-none hover:text-foreground"
                    onClick={() => toggleSort("benchmark")}
                  >
                    {benchmarkHeaderLabel} {sortIndicator("benchmark")}
                  </button>
                </TableHead>
                <TableHead colSpan={configColumnKeys.length} className="text-center">
                  {configGroupLabel}
                </TableHead>
                {metricColumns.map((column) => (
                  <TableHead
                    key={column.key}
                    rowSpan={2}
                    className={cn("align-middle", column.headerClassName)}
                    aria-sort={column.compare ? sortAria(`metric:${column.key}`) : undefined}
                  >
                    {column.compare ? (
                      <button
                        type="button"
                        className={cn(
                          "w-full cursor-pointer select-none hover:text-foreground",
                          column.headerClassName,
                        )}
                        onClick={() => toggleSort(`metric:${column.key}`)}
                      >
                        {column.header} {sortIndicator(`metric:${column.key}`)}
                      </button>
                    ) : (
                      column.header
                    )}
                  </TableHead>
                ))}
              </TableRow>
              <TableRow>
                {configColumnKeys.map((key) => (
                  <TableHead key={key} aria-sort={sortAria(`config:${key}`)}>
                    <button
                      type="button"
                      className="cursor-pointer select-none hover:text-foreground"
                      onClick={() => toggleSort(`config:${key}`)}
                    >
                      {key} {sortIndicator(`config:${key}`)}
                    </button>
                  </TableHead>
                ))}
              </TableRow>
            </>
          ) : (
            <TableRow>
              <TableHead aria-sort={sortAria("benchmark")}>
                <button
                  type="button"
                  className="cursor-pointer select-none hover:text-foreground"
                  onClick={() => toggleSort("benchmark")}
                >
                  {benchmarkHeaderLabel} {sortIndicator("benchmark")}
                </button>
              </TableHead>
              {metricColumns.map((column) => (
                <TableHead
                  key={column.key}
                  className={column.headerClassName}
                  aria-sort={column.compare ? sortAria(`metric:${column.key}`) : undefined}
                >
                  {column.compare ? (
                    <button
                      type="button"
                      className={cn("w-full cursor-pointer select-none hover:text-foreground", column.headerClassName)}
                      onClick={() => toggleSort(`metric:${column.key}`)}
                    >
                      {column.header} {sortIndicator(`metric:${column.key}`)}
                    </button>
                  ) : (
                    column.header
                  )}
                </TableHead>
              ))}
            </TableRow>
          )}
        </TableHeader>
        <TableBody>
          {sortedRows.length === 0 ? (
            <TableRow>
              <TableCell
                colSpan={1 + configColumnKeys.length + metricColumns.length}
                className="py-6 text-center text-muted-foreground"
              >
                {emptyState ?? "No rows to display."}
              </TableCell>
            </TableRow>
          ) : (
            sortedRows.map((row) => (
              <TableRow key={row.key}>
                <TableCell>
                  <Link to={row.benchmarkHref} className="text-primary hover:underline">
                    {row.benchmark}
                  </Link>
                </TableCell>
                {configColumnKeys.map((key) => (
                  <TableCell key={key} className="text-sm">
                    <ConfigValue value={row.config[key]} />
                  </TableCell>
                ))}
                {metricColumns.map((column) => (
                  <TableCell key={column.key} className={column.cellClassName}>
                    {column.render(row)}
                  </TableCell>
                ))}
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </div>
  )
}

function ConfigValue({ value }: { value: string | undefined }) {
  if (!value) return <span>—</span>
  const displayValue = shortenValue(value)
  const valueTitle = displayValue !== value ? value : undefined

  return (
    <span className="text-sm text-foreground" title={valueTitle}>
      {displayValue}
    </span>
  )
}
