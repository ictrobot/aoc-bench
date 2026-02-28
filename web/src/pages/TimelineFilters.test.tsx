import { screen } from "@testing-library/react"
import type { ReactNode } from "react"
import { beforeEach, describe, expect, it, vi } from "vitest"
import { Timeline } from "./Timeline.tsx"
import * as api from "@/lib/api.ts"
import { renderWithRouterAndQueryClient } from "@/test/query-client.tsx"
import { TEST_HOST, makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"
import type { IndexedResults } from "@/lib/types.ts"
import { useLocation } from "react-router-dom"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
  loadResults: vi.fn(),
  decodeResultsForBenchmark: vi.fn(),
}))

vi.mock("recharts", () => {
  const Wrap = ({ children }: { children?: ReactNode }) => <div>{children}</div>
  return {
    ComposedChart: Wrap,
    Bar: Wrap,
    ErrorBar: Wrap,
    XAxis: Wrap,
    YAxis: Wrap,
    CartesianGrid: Wrap,
    Tooltip: Wrap,
    ResponsiveContainer: Wrap,
    Cell: Wrap,
  }
})

vi.mock("@/components/ui/combobox.tsx", () => ({
  Combobox: ({
    value,
    onChange,
    options,
    ariaLabel,
    ariaLabelledBy,
  }: {
    value: string
    onChange: (value: string) => void
    options: { value: string; label: string }[]
    ariaLabel?: string
    ariaLabelledBy?: string
  }) => (
    <select
      aria-label={ariaLabel}
      aria-labelledby={ariaLabelledBy}
      value={value}
      onChange={(e) => onChange(e.target.value)}
    >
      <option value="">Select</option>
      {options.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  ),
}))

vi.mock("@/components/config/ConfigFilter.tsx", () => ({
  ConfigFilter: ({ label, values, value }: { label: string; values: string[]; value: string }) => (
    <div data-testid={`filter-${label}`} data-current-value={value}>
      {values.join("|")}
    </div>
  ),
}))

const HOST = TEST_HOST
const BENCH = "bench-a"
const mockLoadIndex = vi.mocked(api.loadIndex)
const mockLoadResults = vi.mocked(api.loadResults)
const mockDecodeResultsForBenchmark = vi.mocked(api.decodeResultsForBenchmark)

function makeTimelineFiltersHostIndex() {
  return makeHostIndex({
    config_keys: {
      commit: { values: ["a", "b"] },
      mode: { values: ["a", "z"] },
    },
    benchmarks: [{ name: BENCH, result_count: 4 }],
    timeline_key: "commit",
  })
}

function LocationProbe() {
  const location = useLocation()
  return <div data-testid="location">{`${location.pathname}${location.search}`}</div>
}

describe("Timeline filters", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeTimelineFiltersHostIndex()))
    mockLoadResults.mockResolvedValue({ results: [] } satisfies IndexedResults)
  })

  it("passes filter values in canonical index order", async () => {
    mockDecodeResultsForBenchmark.mockReturnValue([
      { bench: "bench-a", config: { commit: "a", mode: "z" }, mean_ns: 100, ci95_half_ns: 1 },
      { bench: "bench-a", config: { commit: "b", mode: "a" }, mean_ns: 101, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(<Timeline />, {
      initialEntries: [`/timeline?host=${HOST}&bench=${BENCH}`],
    })

    expect(await screen.findByTestId("filter-mode")).toHaveTextContent("a|z")
  })

  it("uses f_* URL filters once and removes them from the URL", async () => {
    mockDecodeResultsForBenchmark.mockReturnValue([
      { bench: "bench-a", config: { commit: "a", mode: "z" }, mean_ns: 100, ci95_half_ns: 1 },
      { bench: "bench-a", config: { commit: "b", mode: "a" }, mean_ns: 101, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(
      <>
        <Timeline />
        <LocationProbe />
      </>,
      {
        initialEntries: [`/timeline?host=${HOST}&bench=${BENCH}&f_mode=z`],
      },
    )

    const modeFilter = await screen.findByTestId("filter-mode")
    expect(modeFilter).toHaveAttribute("data-current-value", "z")

    const locationText = screen.getByTestId("location").textContent ?? ""
    expect(locationText).toContain("/timeline")
    expect(locationText).not.toContain("f_mode=")
  })
})
