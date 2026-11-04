import { screen } from "@testing-library/react"
import type { ReactNode } from "react"
import { beforeEach, describe, expect, it, vi } from "vitest"
import { Timeline } from "./Timeline.tsx"
import * as api from "@/lib/api.ts"
import { renderWithRouterAndQueryClient } from "@/test/query-client.tsx"
import { TEST_HOST, makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"
import { useLocation } from "react-router"

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
    mockLoadResults.mockResolvedValue([])
  })

  it("passes filter values in canonical index order", async () => {
    mockDecodeResultsForBenchmark.mockReturnValue([
      {
        bench: "bench-a",
        config: { commit: "a", mode: "z" },
        measurement_token: 0,
        mean_ns: 100,
        ci95_half_ns: 1,
        run_count: 3,
        multimodal: false,
      },
      {
        bench: "bench-a",
        config: { commit: "b", mode: "a" },
        measurement_token: 0,
        mean_ns: 101,
        ci95_half_ns: 1,
        run_count: 3,
        multimodal: false,
      },
    ])

    renderWithRouterAndQueryClient(<Timeline />, {
      initialEntries: [`/timeline?host=${HOST}&bench=${BENCH}`],
    })

    expect(await screen.findByTestId("filter-mode")).toHaveTextContent("a|z")
  })

  it("reads f_* URL filters and keeps them in the URL", async () => {
    mockDecodeResultsForBenchmark.mockReturnValue([
      {
        bench: "bench-a",
        config: { commit: "a", mode: "z" },
        measurement_token: 0,
        mean_ns: 100,
        ci95_half_ns: 1,
        run_count: 3,
        multimodal: false,
      },
      {
        bench: "bench-a",
        config: { commit: "b", mode: "a" },
        measurement_token: 0,
        mean_ns: 101,
        ci95_half_ns: 1,
        run_count: 3,
        multimodal: false,
      },
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
    expect(locationText).toContain("f_mode=z")
  })
})

describe("Timeline breakdown badges", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeTimelineFiltersHostIndex()))
    mockLoadResults.mockResolvedValue([])
  })

  it("marks only multimodal measurements and formats a single run as having no interval", async () => {
    mockDecodeResultsForBenchmark.mockReturnValue([
      {
        bench: "bench-a",
        config: { commit: "a", mode: "z" },
        measurement_token: 0,
        mean_ns: 100,
        ci95_half_ns: null,
        run_count: 1,
        multimodal: false,
      },
      {
        bench: "bench-a",
        config: { commit: "b", mode: "z" },
        measurement_token: 0,
        mean_ns: 120,
        ci95_half_ns: 4,
        run_count: 5,
        multimodal: true,
      },
    ])

    renderWithRouterAndQueryClient(<Timeline />, {
      initialEntries: [`/timeline?host=${HOST}&bench=${BENCH}&f_mode=z`],
    })

    const rows = await screen.findAllByRole("row")
    // header row plus one row per case
    expect(rows).toHaveLength(3)
    expect(screen.getAllByText("multimodal")).toHaveLength(1)
    expect(rows[2]).toHaveTextContent("multimodal")
    expect(rows[1]).not.toHaveTextContent("multimodal")
    expect(rows[1]).toHaveTextContent("no interval")
    expect(rows[2]).toHaveTextContent("±4 ns")
  })
})
