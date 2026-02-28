import { ComposedChart, Line, ErrorBar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"
import { formatDurationNs, formatCi, formatTimestamp } from "@/lib/format.ts"
import type { HistorySeries } from "@/lib/types.ts"

interface HistoryChartProps {
  series: HistorySeries[]
}

export function HistoryChart({ series }: HistoryChartProps) {
  const chartData = series.map((s) => ({
    timestamp: s.timestamp,
    dateLabel: formatTimestamp(s.timestamp),
    mean_ns: s.median_run_mean_ns,
    ci95_half_ns: s.median_run_ci95_half_ns,
    run_count: s.run_count,
  }))

  return (
    <ResponsiveContainer width="100%" height={300}>
      <ComposedChart data={chartData} margin={{ top: 5, right: 20, bottom: 0, left: 20 }}>
        <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
        <XAxis dataKey="dateLabel" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
        <YAxis tickFormatter={(v: number) => formatDurationNs(v)} tick={{ fontSize: 11 }} width={80} />
        <Tooltip content={<HistoryTooltip />} />
        <Line
          type="monotone"
          dataKey="mean_ns"
          stroke="var(--color-chart-1)"
          strokeWidth={1.5}
          dot={{ r: 2 }}
          isAnimationActive={false}
        >
          <ErrorBar
            dataKey="ci95_half_ns"
            width={3}
            strokeWidth={1}
            stroke="var(--color-muted-foreground)"
            direction="y"
          />
        </Line>
      </ComposedChart>
    </ResponsiveContainer>
  )
}

function HistoryTooltip({
  active,
  payload,
}: {
  active?: boolean
  payload?: Array<{
    payload: { dateLabel: string; mean_ns: number; ci95_half_ns: number; run_count: number }
  }>
}) {
  if (!active || !payload?.[0]) return null
  const d = payload[0].payload
  return (
    <div className="rounded-md border bg-background p-3 shadow-md text-sm">
      <div className="font-medium">{d.dateLabel}</div>
      <div>Mean: {formatDurationNs(d.mean_ns)}</div>
      <div className="text-muted-foreground">{formatCi(d.ci95_half_ns)}</div>
      <div className="text-muted-foreground">Runs: {d.run_count}</div>
    </div>
  )
}
