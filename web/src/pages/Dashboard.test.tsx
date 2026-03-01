import { screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
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

function makeDashboardHostIndex() {
  return makeHostIndex({
    config_keys: {
      commit: { values: ["a", "b"] },
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

  it("shows per-benchmark fastest latest time and leaves missing rows blank", async () => {
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "a" }, mean_ns: 100, ci95_half_ns: 1 },
      { bench: "bench-a", config: { commit: "b" }, mean_ns: 50, ci95_half_ns: 1 },
    ])

    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    expect(await screen.findByRole("columnheader", { name: /Time \(fastest config\)/ })).toBeInTheDocument()

    const benchARow = screen.getByRole("link", { name: "bench-a" }).closest("tr")
    const benchBRow = screen.getByRole("link", { name: "bench-b" }).closest("tr")

    expect(benchARow).not.toBeNull()
    expect(benchBRow).not.toBeNull()
    expect(within(benchARow!).getByText("50 ns")).toBeInTheDocument()
    expect(within(benchBRow!).getByText("—")).toBeInTheDocument()
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

  it("formats result counts and sorts by results", async () => {
    const hostIndex = makeHostIndex({
      config_keys: {
        commit: { values: ["a"] },
      },
      benchmarks: [
        { name: "bench-a", result_count: 12_000 },
        { name: "bench-b", result_count: 9 },
        { name: "bench-c", result_count: 345 },
      ],
      timeline_key: "commit",
    })
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([])

    const user = userEvent.setup()
    renderWithRouterAndQueryClient(<Dashboard />, { initialEntries: [`/?host=${HOST}`] })

    const table = await screen.findByRole("table")
    const resultsButton = within(table).getByRole("button", { name: /Results/ })
    const benchmarkOrder = () =>
      within(table)
        .getAllByRole("link")
        .map((link) => link.textContent)

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
