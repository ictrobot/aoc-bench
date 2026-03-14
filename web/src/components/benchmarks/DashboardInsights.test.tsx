import { screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter, useLocation } from "react-router"
import { describe, expect, it } from "vitest"
import { DashboardInsights } from "./DashboardInsights.tsx"
import { renderWithQueryClient } from "@/test/query-client.tsx"
import type { DashboardInsightEntry } from "@/lib/dashboard-insights.ts"

function LocationProbe() {
  const location = useLocation()
  return <div data-testid="location">{location.pathname + location.search}</div>
}

function makeEntry(name: string, mean_ns: number): DashboardInsightEntry {
  return { name, mean_ns, href: `/benchmark?bench=${name}` }
}

describe("DashboardInsights", () => {
  it("shows each fastest and slowest entry as a share of the current total", () => {
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    const fastestCard = screen.getByText("Fastest").closest('[data-slot="card"]') as HTMLElement | null
    const slowestCard = screen.getByText("Slowest").closest('[data-slot="card"]') as HTMLElement | null

    expect(fastestCard).not.toBeNull()
    expect(slowestCard).not.toBeNull()
    expect(within(fastestCard!).getByText("1.3%")).toBeInTheDocument()
    expect(within(fastestCard!).getByText("13%")).toBeInTheDocument()
    expect(within(slowestCard!).getByText("15%")).toBeInTheDocument()
    expect(within(slowestCard!).getByText("3.8%")).toBeInTheDocument()
  })

  it("hides panel shares when every displayed share rounds to 0.0%", () => {
    const entries = Array.from({ length: 3_000 }, (_, i) => makeEntry(`bench-${i + 1}`, 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    const fastestCard = screen.getByText("Fastest").closest('[data-slot="card"]') as HTMLElement | null
    const slowestCard = screen.getByText("Slowest").closest('[data-slot="card"]') as HTMLElement | null

    expect(fastestCard).not.toBeNull()
    expect(slowestCard).not.toBeNull()
    expect(within(fastestCard!).queryByText("0.0%")).toBeNull()
    expect(within(slowestCard!).queryByText("0.0%")).toBeNull()
  })

  it("hides the fastest panel when it would duplicate the slowest benchmark set", () => {
    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={[makeEntry("bench-a", 50), makeEntry("bench-b", 30)]} />
      </MemoryRouter>,
    )

    expect(screen.getByText("Slowest")).toBeInTheDocument()
    expect(screen.queryByText("Fastest")).toBeNull()
  })

  it("shows slowest badges that match the pie slice colors", () => {
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    expect(screen.getByTestId("insight-badge-Slowest-bench-12")).toHaveAttribute(
      "data-color",
      screen.getByLabelText(/^bench-12:/).getAttribute("fill"),
    )
    expect(screen.getByTestId("insight-badge-Slowest-bench-3")).toHaveAttribute(
      "data-color",
      screen.getByLabelText(/^bench-3:/).getAttribute("fill"),
    )
  })

  it("highlights the matching slowest row and offsets the pie slice when a slowest row is hovered", async () => {
    const user = userEvent.setup()
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    const row = screen.getByTestId("insight-row-Slowest-bench-12")
    const slice = screen.getByLabelText(/^bench-12:/)

    await user.hover(row)

    expect(row).toHaveAttribute("data-active", "true")
    expect(slice).toHaveAttribute("data-active", "true")
    expect(slice).toHaveAttribute("transform", expect.stringContaining("translate("))
  })

  it("highlights the matching slowest row when a pie slice is hovered", async () => {
    const user = userEvent.setup()
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    await user.hover(screen.getByLabelText(/^bench-3:/))

    expect(screen.getByTestId("insight-row-Slowest-bench-3")).toHaveAttribute("data-active", "true")
  })

  it("shows a custom tooltip when a pie slice is hovered", async () => {
    const user = userEvent.setup()
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    await user.hover(screen.getByLabelText(/^bench-12:/))

    const tooltip = screen.getByTestId("dashboard-breakdown-tooltip")
    expect(within(tooltip).getByText("bench-12")).toBeInTheDocument()
    expect(within(tooltip).getByText("12 ns")).toBeInTheDocument()
    expect(within(tooltip).getByText("15% of total")).toBeInTheDocument()
    expect(within(tooltip).queryByText("1 benchmark")).toBeNull()
  })

  it("shows the aggregated benchmark count in the other tooltip", async () => {
    const user = userEvent.setup()
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    await user.hover(screen.getByLabelText(/^Other:/))

    const tooltip = screen.getByTestId("dashboard-breakdown-tooltip")
    expect(within(tooltip).getByText("Other")).toBeInTheDocument()
    expect(within(tooltip).getByText("2 benchmarks")).toBeInTheDocument()
  })

  it("starts the first slice at the top and orders slices by time descending with other last", () => {
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
      </MemoryRouter>,
    )

    const slices = [...screen.getAllByLabelText(/^\w/)]
      .filter((el) => el.getAttribute("data-slice-order") != null)
      .sort((a, b) => Number(a.getAttribute("data-slice-order")) - Number(b.getAttribute("data-slice-order")))
    expect(slices.map((slice) => slice.getAttribute("aria-label")?.split(":")[0])).toEqual([
      "bench-12",
      "bench-11",
      "bench-10",
      "bench-9",
      "bench-8",
      "bench-7",
      "bench-6",
      "bench-5",
      "bench-4",
      "bench-3",
      "Other",
    ])
    expect(screen.getByLabelText(/^bench-12:/)).toHaveAttribute("d", expect.stringContaining("L 150 14"))
    expect(screen.getByLabelText(/^bench-12:/)).not.toHaveAttribute(
      "fill",
      screen.getByLabelText(/^Other:/).getAttribute("fill"),
    )
  })

  it("navigates when a pie slice is clicked", async () => {
    const user = userEvent.setup()

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={[makeEntry("bench-a", 50), makeEntry("bench-b", 30)]} />
        <LocationProbe />
      </MemoryRouter>,
    )

    await user.click(screen.getByLabelText(/^bench-b:/))

    expect(screen.getByTestId("location")).toHaveTextContent("/benchmark?bench=bench-b")
  })

  it("renders other as non-navigable when slices are grouped", async () => {
    const user = userEvent.setup()
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    renderWithQueryClient(
      <MemoryRouter initialEntries={["/"]}>
        <DashboardInsights entries={entries} />
        <LocationProbe />
      </MemoryRouter>,
    )

    const totalCard = screen.getByTestId("dashboard-total-time-card")
    expect(within(totalCard).queryByText("Other")).toBeNull()
    expect(within(totalCard).queryByRole("link")).toBeNull()

    await user.click(screen.getByLabelText(/^Other:/))

    expect(screen.getByTestId("location")).toHaveTextContent("/")
  })
})
