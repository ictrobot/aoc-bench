import { fireEvent, render, screen } from "@testing-library/react"
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest"
import { StepTimelineChart, type TimelineChartGroup } from "./StepTimelineChart.tsx"

const WIDTH = 800

function makeGroup(overrides: Partial<TimelineChartGroup>): TimelineChartGroup {
  return {
    fullValue: "a",
    startValue: "a",
    endValue: "a",
    startIndex: 0,
    endIndex: 0,
    mean_ns: 100,
    ci95_half_ns: 2,
    configs: [{ commit: "a" }],
    measurement_token: 1,
    caseCount: 1,
    annotations: [],
    fixedSignature: "",
    color: "blue",
    delta: null,
    ...overrides,
  }
}

// One merged range of two commits followed by a significant regression.
const groups: TimelineChartGroup[] = [
  makeGroup({
    fullValue: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa–bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    startValue: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    endValue: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    endIndex: 1,
    caseCount: 2,
    configs: [
      { commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
      { commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" },
    ],
    annotations: [{ value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", label: "Rust 1.90" }],
  }),
  makeGroup({
    fullValue: "cccccccccccccccccccccccccccccccccccccccc",
    startValue: "cccccccccccccccccccccccccccccccccccccccc",
    endValue: "cccccccccccccccccccccccccccccccccccccccc",
    startIndex: 2,
    endIndex: 2,
    mean_ns: 150,
    configs: [{ commit: "cccccccccccccccccccccccccccccccccccccccc" }],
    color: "red",
    delta: 0.5,
  }),
]

describe("StepTimelineChart", () => {
  // jsdom has no layout, so give the container a real width
  beforeAll(() => {
    Object.defineProperty(HTMLElement.prototype, "clientWidth", { configurable: true, value: WIDTH })
  })
  afterAll(() => {
    // @ts-expect-error restoring jsdom's default
    delete HTMLElement.prototype.clientWidth
  })

  function renderChart(onGroupClick = vi.fn()) {
    render(
      <StepTimelineChart
        groups={groups}
        varyingKey="commit"
        significantGroupIndices={new Set([1])}
        xLabels="sparse"
        onGroupClick={onGroupClick}
      />,
    )
    return onGroupClick
  }

  it("renders the step chart with annotation and significant-group labels", () => {
    renderChart()
    const svg = screen.getByRole("img", { name: "Performance by commit" })
    expect(svg).toBeInTheDocument()
    expect(screen.getByText("Rust 1.90")).toBeInTheDocument()
    // sparse mode labels only the significant group, shortened to 7 chars
    expect(screen.getByText("ccccccc")).toBeInTheDocument()
    expect(screen.queryByText("aaaaaaa")).not.toBeInTheDocument()
    // the significant regression gets a delta label
    expect(screen.getByText("+50%")).toBeInTheDocument()
  })

  it("shows a range tooltip when hovering a merged range", () => {
    renderChart()
    const svg = screen.getByRole("img", { name: "Performance by commit" })
    // slot 0 starts at the left plot edge (x=80 of the 800px wide chart)
    fireEvent.pointerMove(svg, { clientX: 85, clientY: 100 })
    expect(screen.getByText(/1 of 2 identical cases/)).toBeInTheDocument()
    // hovered case in the header plus the range start, both monospace
    expect(screen.getAllByText("aaaaaaa")).toHaveLength(2)
    expect(screen.getByText("bbbbbbb")).toBeInTheDocument()
    expect(screen.getByText(/Mean: 100 ns/)).toBeInTheDocument()
    expect(screen.queryByText(/vs previous/)).not.toBeInTheDocument()

    // hovering the last slot lands on the regression group with its delta
    fireEvent.pointerMove(svg, { clientX: 779, clientY: 100 })
    expect(screen.getByText(/vs previous: \+50.00%/)).toBeInTheDocument()

    fireEvent.pointerLeave(svg)
    expect(screen.queryByText(/Mean:/)).not.toBeInTheDocument()
  })

  it("selects the containing slot, not the nearest slot edge", () => {
    renderChart()
    const svg = screen.getByRole("img", { name: "Performance by commit" })
    // slots are ~233px wide (700px plot / 3); x=290 is 90% through slot 0,
    // which must still select slot 0 (group 0, mean 100), not slot 1
    fireEvent.pointerMove(svg, { clientX: 290, clientY: 100 })
    expect(screen.getByText(/Mean: 100 ns/)).toBeInTheDocument()
  })

  it("ignores pointer positions outside the plot area", () => {
    const onGroupClick = renderChart()
    const svg = screen.getByRole("img", { name: "Performance by commit" })
    // left margin (y-axis labels) and right margin must not select a slot
    fireEvent.click(svg, { clientX: 40, clientY: 100 })
    fireEvent.click(svg, { clientX: 795, clientY: 100 })
    expect(onGroupClick).not.toHaveBeenCalled()

    // moving from the plot into the margin clears the hover state
    fireEvent.pointerMove(svg, { clientX: 85, clientY: 100 })
    expect(screen.getByText(/Mean:/)).toBeInTheDocument()
    fireEvent.pointerMove(svg, { clientX: 40, clientY: 100 })
    expect(screen.queryByText(/Mean:/)).not.toBeInTheDocument()
  })

  it("clamps the CI band at zero when the interval is wider than the mean", () => {
    const wideCi = [
      makeGroup({ mean_ns: 100, ci95_half_ns: 250 }),
      makeGroup({ startValue: "b", endValue: "b", startIndex: 1, endIndex: 1, configs: [{ commit: "b" }] }),
    ]
    const { container } = render(
      <StepTimelineChart
        groups={wideCi}
        varyingKey="commit"
        significantGroupIndices={new Set()}
        xLabels="sparse"
        onGroupClick={vi.fn()}
      />,
    )

    // the axis baseline is the lowest horizontal line; the band must not pass it
    const axisY = Math.max(
      ...[...container.querySelectorAll("line")]
        .filter((l) => l.getAttribute("y1") === l.getAttribute("y2"))
        .map((l) => Number(l.getAttribute("y1"))),
    )
    const band = container.querySelector("path")!.getAttribute("d")!
    const yCoords = [...band.matchAll(/[ML]\s*[-\d.]+\s+([-\d.]+)|V\s*([-\d.]+)/g)].map((m) => Number(m[1] ?? m[2]))
    expect(yCoords.length).toBeGreaterThan(0)
    for (const y of yCoords) {
      expect(y).toBeLessThanOrEqual(axisY)
    }
  })

  it("opens the clicked group with the clicked case preselected", () => {
    const onGroupClick = renderChart()
    const svg = screen.getByRole("img", { name: "Performance by commit" })
    fireEvent.click(svg, { clientX: 779, clientY: 100 })
    expect(onGroupClick).toHaveBeenCalledWith(groups[1], "c".repeat(40))

    fireEvent.click(svg, { clientX: 85, clientY: 100 })
    expect(onGroupClick).toHaveBeenLastCalledWith(groups[0], "a".repeat(40))
  })
})
