import { type MouseEvent as ReactMouseEvent, useRef, useState } from "react"
import { Link, useNavigate } from "react-router"
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { formatDurationNs } from "@/lib/format.ts"
import {
  buildDashboardInsightsData,
  type DashboardInsightEntry,
  type DashboardInsightSlice,
} from "@/lib/dashboard-insights.ts"

const BENCHMARK_SLICE_COLORS = [
  "var(--color-blue-400)",
  "var(--color-red-400)",
  "var(--color-emerald-400)",
  "var(--color-amber-400)",
  "var(--color-violet-400)",
  "var(--color-orange-500)",
  "var(--color-cyan-400)",
  "var(--color-fuchsia-400)",
  "var(--color-teal-500)",
  "var(--color-lime-500)",
]
const OTHER_SLICE_COLOR = "var(--color-gray-400)"
const OTHER_SLICE_NAME = "Other"
const PIE_SIZE = 300
const PIE_CENTER = PIE_SIZE / 2
const PIE_RADIUS = PIE_CENTER - 14
const HOVER_SLICE_OFFSET = 8
const HOVER_SLICE_RADIUS_DELTA = 4
const DIMMED_OPACITY = 0.45

interface DashboardChartSlice extends DashboardInsightSlice {
  order: number
  midAngle: number
  path?: string
  isFullCircle: boolean
  color: string
}

export function DashboardInsights({ entries }: { entries: DashboardInsightEntry[] }) {
  const navigate = useNavigate()
  const [hoveredBenchmark, setHoveredBenchmark] = useState<string | null>(null)
  if (entries.length === 0) return null

  const data = buildDashboardInsightsData(entries)
  const pieSlices = buildPieChartSlices(data.slices)
  const slowestColorByName = new Map(
    pieSlices.filter((slice) => slice.name !== OTHER_SLICE_NAME).map((slice) => [slice.name, slice.color] as const),
  )
  const showFastest = data.benchmarkCount > data.slowest.length

  return (
    <div className={`grid grid-cols-1 gap-4 ${showFastest ? "lg:grid-cols-3" : "lg:grid-cols-2"}`}>
      <Card data-testid="dashboard-total-time-card">
        <CardHeader>
          <CardTitle>Total time</CardTitle>
          <CardDescription>
            {data.benchmarkCount.toLocaleString()} matching benchmark{data.benchmarkCount === 1 ? "" : "s"}
          </CardDescription>
          <CardAction data-testid="dashboard-total-time-value" className="text-2xl font-bold tabular-nums">
            {formatDurationNs(data.totalNs)}
          </CardAction>
        </CardHeader>
        <CardContent>
          <div className="relative h-[300px] overflow-visible" data-testid="dashboard-breakdown-chart">
            <BreakdownPie
              slices={pieSlices}
              onNavigate={navigate}
              activeSliceName={hoveredBenchmark}
              onActiveSliceChange={setHoveredBenchmark}
            />
          </div>
        </CardContent>
      </Card>
      <InsightPanel
        title="Slowest"
        entries={data.slowest}
        totalNs={data.totalNs}
        colorByName={slowestColorByName}
        activeEntryName={hoveredBenchmark}
        onEntryHoverChange={setHoveredBenchmark}
      />
      {showFastest && <InsightPanel title="Fastest" entries={data.fastest} totalNs={data.totalNs} />}
    </div>
  )
}

function InsightPanel({
  title,
  entries,
  totalNs,
  colorByName,
  activeEntryName,
  onEntryHoverChange,
}: {
  title: string
  entries: DashboardInsightEntry[]
  totalNs: number
  colorByName?: ReadonlyMap<string, string>
  activeEntryName?: string | null
  onEntryHoverChange?: (name: string | null) => void
}) {
  if (entries.length === 0) return null
  const shares = entries.map((entry) => formatShare(totalNs === 0 ? 0 : entry.mean_ns / totalNs))
  const showShares = shares.some((share) => share !== "0.0%")

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <ol>
          {entries.map((entry, i) => {
            const badgeColor = colorByName?.get(entry.name)
            const isActive = activeEntryName === entry.name
            const isDimmed = activeEntryName != null && activeEntryName !== entry.name

            return (
              <li key={entry.name} className="border-b last:border-b-0">
                <Link
                  to={entry.href}
                  className={`flex items-center gap-3 px-4 py-2 transition-colors ${isActive ? "bg-muted/70" : "hover:bg-muted/50"} ${isDimmed ? "opacity-60" : ""}`}
                  data-testid={`insight-row-${title}-${entry.name}`}
                  data-active={isActive ? "true" : undefined}
                  onMouseEnter={() => onEntryHoverChange?.(entry.name)}
                  onMouseLeave={() => onEntryHoverChange?.(null)}
                  onFocus={() => onEntryHoverChange?.(entry.name)}
                  onBlur={() => onEntryHoverChange?.(null)}
                >
                  <span className="text-sm text-muted-foreground w-5 text-right shrink-0">{i + 1}.</span>
                  <span className="flex min-w-0 flex-1 items-center gap-2">
                    <span className="truncate text-sm">{entry.name}</span>
                    {badgeColor && (
                      <span
                        className="size-3 shrink-0 rounded-full border border-background/60"
                        style={{ backgroundColor: badgeColor }}
                        data-color={badgeColor}
                        data-testid={`insight-badge-${title}-${entry.name}`}
                        aria-hidden
                      />
                    )}
                  </span>
                  <span className="flex items-baseline gap-2 shrink-0">
                    <span className="text-sm font-mono tabular-nums">{formatDurationNs(entry.mean_ns)}</span>
                    {showShares && <span className="text-xs text-muted-foreground">{shares[i]}</span>}
                  </span>
                </Link>
              </li>
            )
          })}
        </ol>
      </CardContent>
    </Card>
  )
}

function BreakdownPie({
  slices,
  onNavigate,
  activeSliceName,
  onActiveSliceChange,
}: {
  slices: DashboardChartSlice[]
  onNavigate: ReturnType<typeof useNavigate>
  activeSliceName: string | null
  onActiveSliceChange: (name: string | null) => void
}) {
  const tooltipRef = useRef<HTMLDivElement>(null)
  const [pointerOverPie, setPointerOverPie] = useState(false)
  const activeSlice = activeSliceName !== null ? slices.find((slice) => slice.name === activeSliceName) : undefined
  const showTooltip = pointerOverPie && activeSlice != null
  const renderedSlices = activeSlice
    ? [...slices.filter((slice) => slice.name !== activeSlice.name), activeSlice]
    : slices

  const moveTooltip = (event: ReactMouseEvent<SVGElement>) => {
    const el = tooltipRef.current
    const svg = event.currentTarget.ownerSVGElement?.getBoundingClientRect()
    if (!el || !svg) return
    el.style.left = `${event.clientX - svg.left}px`
    el.style.top = `${event.clientY - svg.top}px`
  }

  return (
    <>
      <svg viewBox={`0 0 ${PIE_SIZE} ${PIE_SIZE}`} width="100%" height="100%" aria-label="Benchmark time breakdown">
        {renderedSlices.map((slice) => {
          const isActive = activeSliceName === slice.name
          const isDimmed = activeSliceName !== null && activeSliceName !== slice.name
          const transform = getSliceTransform(slice, isActive)
          const radius = slice.isFullCircle && isActive ? PIE_RADIUS + HOVER_SLICE_RADIUS_DELTA : PIE_RADIUS

          const shared = {
            key: slice.name,
            fill: slice.color,
            stroke: "var(--card)" as const,
            strokeWidth: 1.5,
            opacity: isDimmed ? DIMMED_OPACITY : 1,
            transform,
            "data-slice-order": slice.order,
            "data-active": isActive ? ("true" as const) : undefined,
            "aria-label": formatSliceTitle(slice),
            className: slice.href ? "cursor-pointer" : undefined,
            onClick: () => slice.href && onNavigate(slice.href),
            onMouseEnter: (event: ReactMouseEvent<SVGElement>) => {
              setPointerOverPie(true)
              onActiveSliceChange(slice.name)
              moveTooltip(event)
            },
            onMouseMove: moveTooltip,
            onMouseLeave: () => {
              setPointerOverPie(false)
              onActiveSliceChange(null)
            },
          }

          return slice.isFullCircle ? (
            <circle {...shared} cx={PIE_CENTER} cy={PIE_CENTER} r={radius} />
          ) : (
            <path {...shared} d={slice.path} vectorEffect="non-scaling-stroke" />
          )
        })}
      </svg>
      <div
        ref={tooltipRef}
        className="pointer-events-none absolute z-10 w-fit whitespace-nowrap rounded-md border bg-background/95 px-3 py-2 text-sm shadow-md"
        data-testid="dashboard-breakdown-tooltip"
        style={{
          visibility: showTooltip ? "visible" : "hidden",
          transform: "translate(12px, -50%)",
        }}
      >
        {activeSlice && (
          <>
            <div className="font-medium">{activeSlice.name}</div>
            <div>{formatDurationNs(activeSlice.mean_ns)}</div>
            <div className="text-muted-foreground">{formatShare(activeSlice.share)} of total</div>
            {activeSlice.benchmarkCount > 1 && (
              <div className="text-muted-foreground">{activeSlice.benchmarkCount} benchmarks</div>
            )}
          </>
        )}
      </div>
    </>
  )
}

function formatShare(share: number): string {
  const percent = share * 100
  return `${percent >= 10 ? percent.toFixed(0) : percent.toFixed(1)}%`
}

function formatSliceTitle(slice: DashboardInsightSlice) {
  const benchmarkSummary = slice.benchmarkCount > 1 ? `, ${slice.benchmarkCount} benchmarks` : ""
  return `${slice.name}: ${formatDurationNs(slice.mean_ns)} (${formatShare(slice.share)} of total${benchmarkSummary})`
}

function buildPieChartSlices(slices: DashboardInsightSlice[]): DashboardChartSlice[] {
  const orderedSlices = [
    ...slices
      .filter((slice) => slice.name !== OTHER_SLICE_NAME)
      .sort((a, b) => b.mean_ns - a.mean_ns || a.name.localeCompare(b.name)),
    ...slices.filter((slice) => slice.name === OTHER_SLICE_NAME),
  ]

  let startAngle = -Math.PI / 2
  return orderedSlices.map((slice, order) => {
    const sweepAngle = slice.share * 2 * Math.PI
    const endAngle = startAngle + sweepAngle
    const midAngle = startAngle + sweepAngle / 2
    const isFullCircle = sweepAngle >= Math.PI * 2 - 1e-6
    const path = isFullCircle ? undefined : describePieSlice(startAngle, endAngle)
    const color = slice.name === OTHER_SLICE_NAME ? OTHER_SLICE_COLOR : BENCHMARK_SLICE_COLORS[order]
    startAngle = endAngle
    return { ...slice, order, midAngle, path, isFullCircle, color }
  })
}

function getSliceTransform(slice: DashboardChartSlice, isActive: boolean) {
  if (!isActive || slice.isFullCircle) return undefined

  const dx = formatCoord(HOVER_SLICE_OFFSET * Math.cos(slice.midAngle))
  const dy = formatCoord(HOVER_SLICE_OFFSET * Math.sin(slice.midAngle))
  return `translate(${dx} ${dy})`
}

function describePieSlice(startAngle: number, endAngle: number) {
  const start = polarToCartesian(startAngle)
  const end = polarToCartesian(endAngle)
  const largeArcFlag = endAngle - startAngle > Math.PI ? 1 : 0
  return `M ${PIE_CENTER} ${PIE_CENTER} L ${formatCoord(start.x)} ${formatCoord(start.y)} A ${PIE_RADIUS} ${PIE_RADIUS} 0 ${largeArcFlag} 1 ${formatCoord(end.x)} ${formatCoord(end.y)} Z`
}

function polarToCartesian(angle: number) {
  return {
    x: PIE_CENTER + PIE_RADIUS * Math.cos(angle),
    y: PIE_CENTER + PIE_RADIUS * Math.sin(angle),
  }
}

function formatCoord(value: number) {
  return Number(value.toFixed(3))
}
