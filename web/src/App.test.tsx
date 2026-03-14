import { useState } from "react"
import { render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { Link, useLocation } from "react-router"
import { beforeEach, describe, expect, it, vi } from "vitest"
import * as api from "@/lib/api.ts"
import { TEST_HOST, makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"

const routeProbeState = vi.hoisted(() => ({ enabled: false }))

function RouteStateProbe({ routeName }: { routeName: string }) {
  const location = useLocation()
  const [count, setCount] = useState(0)
  const queryTarget = `${location.pathname}?host=${TEST_HOST}&probe=1`
  const pathTarget = location.pathname === "/" ? `/impact?host=${TEST_HOST}` : `/?host=${TEST_HOST}`

  return (
    <div>
      <div>Probe route: {routeName}</div>
      <div>Count: {count}</div>
      <button type="button" onClick={() => setCount((value) => value + 1)}>
        Increment
      </button>
      <Link to={queryTarget}>Change query</Link>
      <Link to={pathTarget}>Change path</Link>
      <div data-testid="probe-location">{location.pathname + location.search}</div>
    </div>
  )
}

vi.mock("@/components/layout/Header.tsx", () => ({
  Header: () => <div>Header</div>,
}))

vi.mock("@/components/layout/Footer.tsx", () => ({
  Footer: () => <div>Footer</div>,
}))

vi.mock("@/pages/Dashboard.tsx", async () => {
  const actual = await vi.importActual<typeof import("@/pages/Dashboard.tsx")>("@/pages/Dashboard.tsx")
  return {
    Dashboard: () => (routeProbeState.enabled ? <RouteStateProbe routeName="dashboard" /> : <actual.Dashboard />),
  }
})

vi.mock("@/pages/Impact.tsx", async () => {
  const actual = await vi.importActual<typeof import("@/pages/Impact.tsx")>("@/pages/Impact.tsx")
  return {
    Impact: () => (routeProbeState.enabled ? <RouteStateProbe routeName="impact" /> : <actual.Impact />),
  }
})

vi.mock("@/components/config/ConfigFilter.tsx", () => ({
  ConfigFilter: ({
    label,
    values,
    value,
    onChange,
  }: {
    label: string
    values: string[]
    value: string
    onChange: (value: string) => void
  }) => (
    <label>
      {label}:
      <select aria-label={`${label}:`} value={value} onChange={(event) => onChange(event.target.value)}>
        <option value="">All</option>
        {values.map((entry) => (
          <option key={entry} value={entry}>
            {entry}
          </option>
        ))}
      </select>
    </label>
  ),
}))

vi.mock("@/lib/api.ts", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api.ts")>("@/lib/api.ts")
  return {
    ...actual,
    loadIndex: vi.fn(),
    decodeLatestResults: vi.fn(),
  }
})

import App from "./App.tsx"

const mockLoadIndex = vi.mocked(api.loadIndex)
const mockDecodeLatestResults = vi.mocked(api.decodeLatestResults)

function makeDashboardHostIndex() {
  return makeHostIndex({
    config_keys: {
      commit: { values: ["a", "b"] },
      build: { values: ["safe", "fast"] },
      threads: { values: ["1", "2"] },
    },
    benchmarks: [
      { name: "bench-a", result_count: 2 },
      { name: "bench-b", result_count: 2 },
      { name: "bench-c", result_count: 2 },
    ],
    timeline_key: "commit",
  })
}

describe("App", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    routeProbeState.enabled = false
    Object.defineProperty(window, "matchMedia", {
      writable: true,
      value: vi.fn().mockReturnValue({
        matches: false,
        media: "",
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }),
    })
    window.history.replaceState({}, "", `/?host=${TEST_HOST}`)
  })

  it("preserves route-local state for same-path query changes but resets it on pathname changes", async () => {
    const user = userEvent.setup()
    routeProbeState.enabled = true
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeDashboardHostIndex()))
    mockDecodeLatestResults.mockReturnValue([])

    render(<App />)

    expect(await screen.findByText("Probe route: dashboard")).toBeInTheDocument()
    expect(screen.getByText("Count: 0")).toBeInTheDocument()

    await user.click(screen.getByRole("button", { name: "Increment" }))
    expect(screen.getByText("Count: 1")).toBeInTheDocument()

    await user.click(screen.getByRole("link", { name: "Change query" }))
    expect(await screen.findByText("Probe route: dashboard")).toBeInTheDocument()
    expect(screen.getByText("Count: 1")).toBeInTheDocument()
    expect(screen.getByTestId("probe-location")).toHaveTextContent(`/?host=${TEST_HOST}&probe=1`)

    await user.click(screen.getByRole("link", { name: "Change path" }))
    expect(await screen.findByText("Probe route: impact")).toBeInTheDocument()
    expect(screen.getByText("Count: 0")).toBeInTheDocument()
  })

  it("preserves dashboard sort state when filters change on the same page", async () => {
    const user = userEvent.setup()
    const hostIndex = makeDashboardHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeLatestResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "b", build: "fast", threads: "1" }, mean_ns: 50, ci95_half_ns: 1 },
      { bench: "bench-b", config: { commit: "b", build: "safe", threads: "1" }, mean_ns: 60, ci95_half_ns: 1 },
      { bench: "bench-c", config: { commit: "b", build: "safe", threads: "2" }, mean_ns: 70, ci95_half_ns: 1 },
    ])

    render(<App />)

    await screen.findByRole("columnheader", { name: /build/ })
    const table = screen.getByRole("table")
    const buildHeader = within(table).getByRole("columnheader", { name: /build/ })
    const buildButton = within(table).getByRole("button", { name: /build/ })
    const benchmarkOrder = () =>
      within(screen.getByRole("table"))
        .getAllByRole("link")
        .map((link) => link.textContent)

    await user.click(buildButton)
    expect(buildHeader).toHaveAttribute("aria-sort", "ascending")
    expect(benchmarkOrder()).toEqual(["bench-b", "bench-c", "bench-a"])

    await user.selectOptions(screen.getByLabelText("threads:"), "1")

    const filteredBuildHeader = within(screen.getByRole("table")).getByRole("columnheader", { name: /build/ })
    expect(filteredBuildHeader).toHaveAttribute("aria-sort", "ascending")
    expect(benchmarkOrder()).toEqual(["bench-b", "bench-a"])
  })
})
