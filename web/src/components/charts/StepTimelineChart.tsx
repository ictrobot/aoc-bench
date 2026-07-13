import React, { useLayoutEffect, useMemo, useRef, useState } from "react"
import { formatCi, formatDurationNs, shortenValue } from "@/lib/format.ts"
import type { TimelineResultGroup } from "@/lib/timeline-grouping.ts"
import { GroupValueLabel } from "@/components/config/GroupValueLabel.tsx"

export type TimelineChartGroup = TimelineResultGroup & {
  color: string
  delta: number | null
}

interface StepTimelineChartProps {
  groups: TimelineChartGroup[]
  varyingKey: string
  significantGroupIndices: Set<number>
  /** "sparse" labels only significant groups (overview); "all" labels every range start (detail) */
  xLabels: "sparse" | "all"
  /** Called with the clicked group and the exact case value that was clicked */
  onGroupClick: (group: TimelineChartGroup, value?: string) => void
}

const LINE_COLOR = "var(--color-chart-1)"
const BAND_COLOR = "color-mix(in oklab, var(--color-chart-1) 15%, transparent)"
const MARGIN_LEFT = 80
const MARGIN_RIGHT = 20
const MARGIN_TOP = 24
const PLOT_HEIGHT = 320
const LABEL_BAND = 84
const MAX_DELTA_LABELS = 5

const TICK_FONT_SIZE = 13
const ANNOTATION_FONT_SIZE = 12
const DELTA_FONT_SIZE = 13

interface HoverState {
  slot: number
  groupIndex: number
  /** cursor y within the container, so the tooltip can follow the pointer */
  pointerY: number
}

/**
 * From-0 step-line chart for piecewise-constant per-case measurements.
 * Each case occupies one equal-width slot; a merged range of identical
 * measurements draws as a flat run. Change markers, the hover crosshair,
 * and the hover dot all anchor to slot left edges (the step risers).
 */
export function StepTimelineChart({
  groups,
  varyingKey,
  significantGroupIndices,
  xLabels,
  onGroupClick,
}: StepTimelineChartProps) {
  const [containerRef, width] = useContainerWidth()
  const [hover, setHover] = useState<HoverState | null>(null)

  const hasXLabels = xLabels === "all" || significantGroupIndices.size > 0
  const height = MARGIN_TOP + PLOT_HEIGHT + (hasXLabels ? LABEL_BAND : 12)
  const plotWidth = Math.max(0, width - MARGIN_LEFT - MARGIN_RIGHT)
  const plotBottom = MARGIN_TOP + PLOT_HEIGHT

  const layout = useMemo(() => {
    const groupStartSlot: number[] = []
    const slotToGroup: number[] = []
    let slots = 0
    groups.forEach((group, groupIndex) => {
      groupStartSlot.push(slots)
      for (let i = 0; i < group.caseCount; i++) slotToGroup.push(groupIndex)
      slots += group.caseCount
    })
    let dataMax = 0
    for (const g of groups) dataMax = Math.max(dataMax, g.mean_ns + g.ci95_half_ns)
    const yMax = dataMax * 1.05 || 1
    return { groupStartSlot, slotToGroup, slots, yMax }
  }, [groups])

  const { x, y } = useMemo(() => {
    return {
      x: (slot: number) => MARGIN_LEFT + (plotWidth * slot) / layout.slots,
      y: (ns: number) => MARGIN_TOP + PLOT_HEIGHT * (1 - ns / layout.yMax),
    }
  }, [layout, plotWidth])

  const paths = useMemo(() => {
    if (plotWidth <= 0 || groups.length === 0) return null
    let line = `M ${x(0)} ${y(groups[0].mean_ns)}`
    let bandTop = `M ${x(0)} ${y(groups[0].mean_ns + groups[0].ci95_half_ns)}`
    let bandBottom = ""
    groups.forEach((group, i) => {
      const endX = x(layout.groupStartSlot[i] + group.caseCount)
      line += ` H ${endX}`
      bandTop += ` H ${endX}`
      const next = groups[i + 1]
      if (next) {
        if (next.mean_ns !== group.mean_ns) line += ` V ${y(next.mean_ns)}`
        bandTop += ` V ${y(next.mean_ns + next.ci95_half_ns)}`
      }
    })
    for (let i = groups.length - 1; i >= 0; i--) {
      const group = groups[i]
      const startX = x(layout.groupStartSlot[i])
      const endX = x(layout.groupStartSlot[i] + group.caseCount)
      // a CI half-width larger than the mean must not extend below the axis
      const yLow = y(Math.max(0, group.mean_ns - group.ci95_half_ns))
      bandBottom += ` L ${endX} ${yLow} H ${startX}`
    }
    return { line, band: `${bandTop}${bandBottom} Z` }
  }, [groups, layout, plotWidth, x, y])

  const yTicks = useMemo(() => niceTicks(layout.yMax), [layout.yMax])

  // Change markers: significant groups get a dot on the step riser; the
  // largest few also get a delta label. Each label tries right-of-dot,
  // left-of-dot, then a staggered row, taking the first position that
  // doesn't collide with another marker dot or an already-placed label.
  const deltaLabels = useMemo(() => {
    type Box = { x0: number; x1: number; y0: number; y1: number }
    const overlaps = (a: Box, b: Box) => a.x0 < b.x1 && a.x1 > b.x0 && a.y0 < b.y1 && a.y1 > b.y0

    const markerIndices = [...significantGroupIndices].filter((i) => groups[i]?.delta !== null)
    const obstacles: Box[] = markerIndices.map((i) => {
      const cx = x(layout.groupStartSlot[i])
      const cy = y(groups[i].mean_ns)
      return { x0: cx - 6, x1: cx + 6, y0: cy - 6, y1: cy + 6 }
    })

    const placement = new Map<number, { left: boolean; row: number }>()
    markerIndices
      .sort((a, b) => Math.abs(groups[b].delta ?? 0) - Math.abs(groups[a].delta ?? 0))
      .slice(0, MAX_DELTA_LABELS)
      .sort((a, b) => layout.groupStartSlot[a] - layout.groupStartSlot[b])
      .forEach((groupIndex) => {
        const delta = groups[groupIndex].delta ?? 0
        const cx = x(layout.groupStartSlot[groupIndex])
        const cy = y(groups[groupIndex].mean_ns)
        const text = `${delta > 0 ? "+" : ""}${(delta * 100).toFixed(0)}%`
        const textWidth = text.length * DELTA_FONT_SIZE * 0.62
        const nearRight = cx > width - MARGIN_RIGHT - textWidth - 12

        const candidates: { left: boolean; row: number }[] = [
          { left: nearRight, row: 0 },
          { left: !nearRight, row: 0 },
          { left: nearRight, row: 1 },
          { left: !nearRight, row: 1 },
        ]
        const boxFor = ({ left, row }: { left: boolean; row: number }): Box => {
          const x1 = left ? cx - 6 : cx + 6 + textWidth
          const baseline = cy + (delta > 0 ? -9 - row * 16 : 16 + row * 16)
          return { x0: x1 - textWidth, x1, y0: baseline - DELTA_FONT_SIZE, y1: baseline + 2 }
        }
        const chosen =
          candidates.find((c) => {
            const box = boxFor(c)
            return box.x0 >= MARGIN_LEFT && !obstacles.some((o) => overlaps(box, o))
          }) ?? candidates[candidates.length - 1]
        obstacles.push(boxFor(chosen))
        placement.set(groupIndex, chosen)
      })
    return placement
  }, [significantGroupIndices, groups, layout, width, x, y])

  // Annotated cases (e.g. release tags) as labeled vertical hairlines.
  // Labels that would collide with the previous label are dropped (the
  // hairline is still drawn, and the tooltip names the annotation); labels
  // near the right edge flip to the left side of their hairline.
  const annotationLines = useMemo(() => {
    const lines: { slot: number; label: string; showLabel: boolean; flip: boolean }[] = []
    let labelEnd = -Infinity
    groups.forEach((group, groupIndex) => {
      for (const annotation of group.annotations) {
        const caseIndex = group.configs.findIndex((config) => config[varyingKey] === annotation.value)
        if (caseIndex < 0) continue
        const slot = layout.groupStartSlot[groupIndex] + caseIndex
        const labelWidth = annotation.label.length * (ANNOTATION_FONT_SIZE * 0.55)
        const flip = x(slot) + 4 + labelWidth > width - MARGIN_RIGHT
        const startX = flip ? x(slot) - 4 - labelWidth : x(slot) + 4
        const showLabel = startX >= labelEnd
        if (showLabel) labelEnd = startX + labelWidth + 8
        lines.push({ slot, label: annotation.label, showLabel, flip })
      }
    })
    return lines
  }, [groups, layout, varyingKey, width, x])

  function slotFromEvent(event: React.MouseEvent<SVGSVGElement>): number | null {
    const rect = event.currentTarget.getBoundingClientRect()
    if (plotWidth <= 0) return null
    const px = event.clientX - rect.left
    if (px < MARGIN_LEFT || px > MARGIN_LEFT + plotWidth) return null
    const slot = Math.floor(((px - MARGIN_LEFT) / plotWidth) * layout.slots)
    return Math.min(layout.slots - 1, Math.max(0, slot))
  }

  function onPointerMove(event: React.PointerEvent<SVGSVGElement>) {
    const slot = slotFromEvent(event)
    if (slot === null) {
      setHover(null)
      return
    }
    const pointerY = event.clientY - event.currentTarget.getBoundingClientRect().top
    setHover({ slot, groupIndex: layout.slotToGroup[slot], pointerY })
  }

  function onClick(event: React.MouseEvent<SVGSVGElement>) {
    const slot = slotFromEvent(event)
    if (slot === null) return
    const groupIndex = layout.slotToGroup[slot]
    const group = groups[groupIndex]
    onGroupClick(group, group.configs[slot - layout.groupStartSlot[groupIndex]]?.[varyingKey])
  }

  const hoverGroup = hover ? groups[hover.groupIndex] : null

  return (
    <div ref={containerRef} className="relative w-full">
      {width > 0 && paths && (
        <svg
          width={width}
          height={height}
          className="block cursor-pointer"
          role="img"
          aria-label={`Performance by ${varyingKey}`}
          onPointerMove={onPointerMove}
          onPointerLeave={() => setHover(null)}
          onClick={onClick}
        >
          {/* gridlines + y-axis ticks */}
          {yTicks.map((t) => (
            <g key={t}>
              <line
                x1={MARGIN_LEFT}
                x2={width - MARGIN_RIGHT}
                y1={y(t)}
                y2={y(t)}
                stroke="var(--color-border)"
                strokeWidth={1}
              />
              <text
                x={MARGIN_LEFT - 8}
                y={y(t) + 4}
                textAnchor="end"
                fontSize={TICK_FONT_SIZE}
                fill="var(--color-muted-foreground)"
              >
                {formatDurationNs(t)}
              </text>
            </g>
          ))}

          {/* annotation hairlines */}
          {annotationLines.map((a) => (
            <g key={`${a.slot}-${a.label}`}>
              <line
                x1={x(a.slot)}
                x2={x(a.slot)}
                y1={MARGIN_TOP}
                y2={plotBottom}
                stroke="var(--color-muted-foreground)"
                strokeOpacity={0.5}
                strokeWidth={1}
              />
              {a.showLabel && (
                <text
                  x={x(a.slot) + (a.flip ? -4 : 4)}
                  y={MARGIN_TOP - 8}
                  textAnchor={a.flip ? "end" : "start"}
                  fontSize={ANNOTATION_FONT_SIZE}
                  fill="var(--color-muted-foreground)"
                >
                  {a.label}
                </text>
              )}
            </g>
          ))}

          {/* CI band + step line */}
          <path d={paths.band} fill={BAND_COLOR} />
          <path d={paths.line} fill="none" stroke={LINE_COLOR} strokeWidth={2} strokeLinejoin="round" />

          {/* hovered range highlight */}
          {hover && hoverGroup && (
            <path
              d={`M ${x(layout.groupStartSlot[hover.groupIndex])} ${y(hoverGroup.mean_ns)} H ${x(
                layout.groupStartSlot[hover.groupIndex] + hoverGroup.caseCount,
              )}`}
              fill="none"
              stroke={LINE_COLOR}
              strokeWidth={4}
              strokeLinecap="round"
            />
          )}

          {/* change-point markers on step risers */}
          {[...significantGroupIndices].map((groupIndex) => {
            const group = groups[groupIndex]
            if (!group || group.delta === null) return null
            const cx = x(layout.groupStartSlot[groupIndex])
            const cy = y(group.mean_ns)
            const label = deltaLabels.get(groupIndex)
            return (
              <g key={groupIndex}>
                <circle cx={cx} cy={cy} r={4.5} fill={group.color} stroke="var(--color-card)" strokeWidth={2} />
                {label && (
                  <text
                    x={cx + (label.left ? -6 : 6)}
                    y={cy + (group.delta > 0 ? -9 - label.row * 16 : 16 + label.row * 16)}
                    textAnchor={label.left ? "end" : "start"}
                    fontSize={DELTA_FONT_SIZE}
                    fontWeight={600}
                    fill={group.color}
                  >
                    {`${group.delta > 0 ? "+" : ""}${(group.delta * 100).toFixed(0)}%`}
                  </text>
                )}
              </g>
            )
          })}

          {/* x-axis baseline + labels */}
          <line
            x1={MARGIN_LEFT}
            x2={width - MARGIN_RIGHT}
            y1={plotBottom}
            y2={plotBottom}
            stroke="var(--color-border)"
            strokeWidth={1}
          />
          {groups.map((group, groupIndex) => {
            if (xLabels === "sparse" && !significantGroupIndices.has(groupIndex)) return null
            const gx = x(layout.groupStartSlot[groupIndex])
            return (
              <text
                key={groupIndex}
                transform={`translate(${gx},${plotBottom + 14}) rotate(-45)`}
                textAnchor="end"
                fontSize={TICK_FONT_SIZE}
                fill="var(--color-muted-foreground)"
              >
                {shortenValue(group.startValue)}
              </text>
            )
          })}

          {/* hover crosshair + dot */}
          {hover && hoverGroup && (
            <g pointerEvents="none">
              <line
                x1={x(hover.slot)}
                x2={x(hover.slot)}
                y1={MARGIN_TOP}
                y2={plotBottom}
                stroke="var(--color-muted-foreground)"
                strokeWidth={1}
              />
              <circle
                cx={x(hover.slot)}
                cy={y(hoverGroup.mean_ns)}
                r={4}
                fill={LINE_COLOR}
                stroke="var(--color-card)"
                strokeWidth={2}
              />
            </g>
          )}
        </svg>
      )}

      {hover && hoverGroup && (
        <div
          className="absolute z-10 pointer-events-none rounded-md border bg-background p-3 shadow-md text-sm whitespace-nowrap"
          style={{
            ...(x(hover.slot) > width * 0.6 ? { right: width - x(hover.slot) + 14 } : { left: x(hover.slot) + 14 }),
            ...(hover.pointerY > height * 0.6
              ? { bottom: height - hover.pointerY + 14 }
              : { top: hover.pointerY + 14 }),
          }}
        >
          <div className="font-medium font-mono">
            {shortenValue(hoverGroup.configs[hover.slot - layout.groupStartSlot[hover.groupIndex]]?.[varyingKey] ?? "")}
          </div>
          {hoverGroup.caseCount > 1 && (
            <div className="text-muted-foreground">
              1 of {hoverGroup.caseCount} identical cases (<GroupValueLabel group={hoverGroup} />)
            </div>
          )}
          {hoverGroup.annotations.map((annotation) => (
            <div key={annotation.value}>
              {shortenValue(annotation.value)}: {annotation.label}
            </div>
          ))}
          <div>
            Mean: {formatDurationNs(hoverGroup.mean_ns)}
            <span className="text-muted-foreground"> {formatCi(hoverGroup.ci95_half_ns)}</span>
          </div>
          {hoverGroup.delta !== null && (
            <div className="text-muted-foreground">
              vs previous: {hoverGroup.delta > 0 ? "+" : ""}
              {(hoverGroup.delta * 100).toFixed(2)}%
            </div>
          )}
        </div>
      )}
    </div>
  )
}

/** ~4-5 solid gridlines at a round step, always starting from 0 */
function niceTicks(max: number): number[] {
  const raw = max / 4
  const magnitude = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => max / s <= 5) ?? 10 * magnitude
  const ticks: number[] = []
  for (let i = 0; i * step <= max; i++) ticks.push(i * step)
  return ticks
}

function useContainerWidth(): [React.RefObject<HTMLDivElement | null>, number] {
  const ref = useRef<HTMLDivElement>(null)
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    setWidth(el.clientWidth)
    if (typeof ResizeObserver === "undefined") return
    const observer = new ResizeObserver(() => setWidth(el.clientWidth))
    observer.observe(el)
    return () => observer.disconnect()
  }, [])
  return [ref, width]
}
