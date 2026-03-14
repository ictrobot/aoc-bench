import { screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { useLocation } from "react-router"
import { beforeEach, describe, expect, it, vi } from "vitest"
import { Dashboard } from "./Dashboard.tsx"
import * as api from "@/lib/api.ts"
import { renderWithRouterAndQueryClient } from "@/test/query-client.tsx"
import { TEST_HOST, makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
  decodeLatestResults: vi.fn(),
}))

const HOST = TEST_HOST
const mockLoadIndex = vi.mocked(api.loadIndex)
const mockDecodeLatestResults = vi.mocked(api.decodeLatestResults)

function LocationProbe() {
  const location = useLocation()
  return <div data-testid="location">{location.pathname + location.search}</div>
}

function makeDashboardHostIndex() {
  return makeHostIndex({
    config_keys: {
      commit: { values: ["a", "b"] },
      build: { values: ["safe", "fast"] },
    },
    benchmarks: [
      { name: "bench-a", result_count: 2 },
      { name: "bench-b", result_count: 1 },
    ],
    timeline_key: "commit",
  })
}

describe("Dashboard", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("shows fastest config and time columns for timeline hosts", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "safe" }, mean_ns: 100, ci95_half_ns: 1 },
      { bench: "bench-a", config: { commit: "b", build: "fast" }, mean_ns: 50, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    expect(await screen.findByRole("columnheader", { name: /Fastest config/ })).toBeInTheDocument()
    expect(screen.getByRole("columnheader", { name: /Benchmark/ })).toBeInTheDocument()
    expect(screen.getByRole("columnheader", { name: /Fastest config/ })).toBeInTheDocument()
    expect(screen.getByRole("columnheader", { name: /build/ })).toBeInTheDocument()
    expect(screen.getByRole("columnheader", { name: /Time/ })).toBeInTheDocument()
    expect(screen.queryByRole("columnheader", { name: /Result count/ })).toBeNull()
    expect(screen.getByText((_, element) => element?.textContent === "Current commit: b")).toBeInTheDocument()
    expect(screen.getByText("Total time")).toBeInTheDocument()
    expect(screen.getByTestId("dashboard-total-time-value")).toHaveTextContent("50 ns")
    expect(screen.getByText("1 matching benchmark")).toBeInTheDocument()
    expect(screen.getByText("Slowest")).toBeInTheDocument()
    expect(screen.queryByText("Fastest")).toBeNull()

    const benchARow = screen.getByRole("link", { name: "bench-a" }).closest("tr")
    const benchBRow = screen.getByRole("link", { name: "bench-b" }).closest("tr")

    expect(benchARow).not.toBeNull()
    expect(benchBRow).not.toBeNull()
    expect(await within(benchARow!).findByText("fast")).toBeInTheDocument()
    expect(within(benchARow!).getByText("50 ns")).toBeInTheDocument()
    expect(screen.queryByRole("columnheader", { name: "commit" })).toBeNull()
    expect(within(benchBRow!).getAllByText("—")).toHaveLength(2)
  })

  it("breaks ties for fastest results using the canonical config value order", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "fast" }, mean_ns: 50, ci95_half_ns: 1 },
      { bench: "bench-a", config: { commit: "b", build: "safe" }, mean_ns: 50, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    expect(await screen.findByRole("columnheader", { name: /build/ })).toBeInTheDocument()
    const benchARow = (await screen.findByRole("link", { name: "bench-a" })).closest("tr")
    expect(benchARow).not.toBeNull()
    expect(within(benchARow!).getByText("safe")).toBeInTheDocument()
    expect(within(benchARow!).queryByText("fast")).toBeNull()
    expect(within(benchARow!).getByText("50 ns")).toBeInTheDocument()
  })

  it("sums matching fastest-result times in the total time panel", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "fast" }, mean_ns: 50, ci95_half_ns: 1 },
      { bench: "bench-b", config: { commit: "b", build: "safe" }, mean_ns: 90, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    expect(await screen.findByText("Total time")).toBeInTheDocument()
    expect(screen.getByTestId("dashboard-total-time-value")).toHaveTextContent("140 ns")
    expect(screen.getByText("2 matching benchmarks")).toBeInTheDocument()
  })

  it("falls back to the result count table when a timeline host has no latest results", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    const table = await screen.findByRole("table")
    expect(within(table).getByRole("columnheader", { name: /Result count/ })).toBeInTheDocument()
    expect(screen.queryByRole("columnheader", { name: /Fastest config/ })).toBeNull()
    expect(screen.queryByRole("columnheader", { name: /^Time$/ })).toBeNull()
    expect(screen.queryByRole("columnheader", { name: /build/ })).toBeNull()
    expect(screen.queryByText(/Current commit:/)).toBeNull()
    expect(screen.queryByText("Total time")).toBeNull()
    expect(screen.queryByText("Slowest")).toBeNull()
    expect(screen.queryByText("Fastest")).toBeNull()

    const benchARow = screen.getByRole("link", { name: "bench-a" }).closest("tr")
    expect(benchARow).not.toBeNull()
    expect(within(benchARow!).getByText("2")).toBeInTheDocument()
  })

  it("hides config columns entirely when all non-timeline keys are filtered", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "fast" }, mean_ns: 50, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}&f_build=fast`] })

    expect(await screen.findByText((_, element) => element?.textContent === "Current commit: b")).toBeInTheDocument()
    const benchARow = (await screen.findByRole("link", { name: "bench-a" })).closest("tr")
    expect(benchARow).not.toBeNull()
    expect(screen.queryByRole("columnheader", { name: /build/ })).toBeNull()
    expect(screen.queryByRole("columnheader", { name: "commit" })).toBeNull()
    expect(within(benchARow!).queryByText("fast")).toBeNull()
    expect(within(benchARow!).getByText("50 ns")).toBeInTheDocument()
  })

  it("ignores and removes any timeline key filter from the dashboard URL", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "fast" }, mean_ns: 50, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(
      <>
        <Dashboard />
        <LocationProbe />
      </>,
      { initialEntries: [`/?host=${HOST}&f_commit=a&f_build=fast`] },
    )

    expect(await screen.findByText((_, element) => element?.textContent === "Current commit: b")).toBeInTheDocument()
    const benchARow = (await screen.findByRole("link", { name: "bench-a" })).closest("tr")
    expect(benchARow).not.toBeNull()
    expect(within(benchARow!).getByText("50 ns")).toBeInTheDocument()

    const locationText = await screen.findByTestId("location")
    expect(locationText).toHaveTextContent(`/?host=${HOST}&f_build=fast`)
    expect(locationText.textContent).not.toContain("f_commit=")
  })

  it("hides benchmarks that do not match the active homepage filters", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "fast" }, mean_ns: 50, ci95_half_ns: 1 },
      { bench: "bench-b", config: { commit: "b", build: "safe" }, mean_ns: 90, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}&f_build=fast`] })

    expect(await screen.findByText((_, element) => element?.textContent === "Current commit: b")).toBeInTheDocument()
    expect(screen.getByTestId("dashboard-total-time-value")).toHaveTextContent("50 ns")
    expect(await screen.findByRole("link", { name: "bench-a" })).toBeInTheDocument()
    expect(screen.queryByRole("link", { name: "bench-b" })).toBeNull()
    expect(screen.queryByRole("button", { name: /Open benchmark bench-b/ })).toBeNull()
  })

  it("shows the default empty state when homepage filters match no fastest configs", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "safe" }, mean_ns: 50, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(
      <>
        <Dashboard />
        <LocationProbe />
      </>,
      { initialEntries: [`/?host=${HOST}&f_build=fast`] },
    )

    expect(await screen.findByText((_, element) => element?.textContent === "Current commit: b")).toBeInTheDocument()
    expect(screen.getByText("No rows to display.")).toBeInTheDocument()
    expect(screen.queryByText("Total time")).toBeNull()
    expect(screen.queryByRole("link", { name: "bench-a" })).toBeNull()
    expect(screen.getByTestId("location")).toHaveTextContent(`/?host=${HOST}&f_build=fast`)
  })

  it("sorts config columns by the config value order", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "safe" }, mean_ns: 80, ci95_half_ns: 1 },
      { bench: "bench-b", config: { commit: "b", build: "fast" }, mean_ns: 50, ci95_half_ns: 1 },
    ])

    const user = userEvent.setup()
    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    await screen.findByRole("columnheader", { name: /build/ })
    const table = screen.getByRole("table")
    const buildButton = within(table).getByRole("button", { name: /build/ })
    const benchmarkOrder = () =>
      within(table)
        .getAllByRole("link")
        .map((link) => link.textContent)

    expect(benchmarkOrder()).toEqual(["bench-a", "bench-b"])

    await user.click(buildButton)
    expect(benchmarkOrder()).toEqual(["bench-a", "bench-b"])

    await user.click(buildButton)
    expect(benchmarkOrder()).toEqual(["bench-b", "bench-a"])
  })

  it("shows decode error and suppresses benchmark cards/table", async () => {
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeDashboardHostIndex()))
    mockDecodeLatestResults.mockImplementation(() => {
      throw new Error("decode failed")
    })

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    expect(await screen.findByText("Error: decode failed")).toBeInTheDocument()
    expect(screen.queryByRole("table")).toBeNull()
  })

  it("shows host description with links when present", async () => {
    const hostIndex = makeDashboardHostIndex()
    hostIndex.description = "[Example](https://example.com) instance."
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    const link = await screen.findByRole("link", { name: "Example" })
    expect(link).toHaveAttribute("href", "https://example.com")
    expect(link.closest("p")?.textContent).toBe("Example instance.")
  })

  it("does not show description when not present", async () => {
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeDashboardHostIndex()))
    mockDecodeLatestResults.mockReturnValue([])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    await screen.findByText(HOST)
    expect(screen.queryByText("instance")).toBeNull()
  })

  it("keeps the result count table when there is no timeline key", async () => {
    const hostIndex = makeHostIndex({
      config_keys: {
        build: { values: ["native"] },
      },
      benchmarks: [
        { name: "bench-a", result_count: 12_000 },
        { name: "bench-b", result_count: 9 },
        { name: "bench-c", result_count: 345 },
      ],
    })
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue(null)

    const user = userEvent.setup()
    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    const table = await screen.findByRole("table")
    const resultsButton = within(table).getByRole("button", { name: /Result count/ })
    const benchmarkOrder = () =>
      within(table)
        .getAllByRole("link")
        .map((link) => link.textContent)

    expect(screen.queryByRole("columnheader", { name: /Fastest config/ })).toBeNull()
    expect(screen.queryByRole("columnheader", { name: /^Time$/ })).toBeNull()
    expect(screen.queryByText("Total time")).toBeNull()
    expect(screen.queryByText("Slowest")).toBeNull()
    expect(screen.queryByText("Fastest")).toBeNull()

    const benchARow = screen.getByRole("link", { name: "bench-a" }).closest("tr")
    expect(benchARow).not.toBeNull()
    expect(within(benchARow!).getByText((12_000).toLocaleString())).toBeInTheDocument()

    expect(benchmarkOrder()).toEqual(["bench-a", "bench-b", "bench-c"])

    await user.click(resultsButton)
    expect(benchmarkOrder()).toEqual(["bench-b", "bench-c", "bench-a"])

    await user.click(resultsButton)
    expect(benchmarkOrder()).toEqual(["bench-a", "bench-c", "bench-b"])
  })
})
