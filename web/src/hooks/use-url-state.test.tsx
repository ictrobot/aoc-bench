import { screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import type { ReactElement } from "react"
import { MemoryRouter, useLocation, useNavigationType } from "react-router"
import { beforeEach, describe, expect, it, vi } from "vitest"
import * as api from "@/lib/api.ts"
import { useSetUrlParams, useUrlFilters, useUrlHostBenchmark, useUrlParam } from "@/hooks/use-url-state.tsx"
import { makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"
import { renderWithQueryClient, renderWithRouterAndQueryClient } from "@/test/query-client.tsx"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
}))

const HOST = "linux-x64"
const BENCH = "bench-a"
const mockLoadIndex = vi.mocked(api.loadIndex)

function makeUrlStateHostIndex() {
  return makeHostIndex({
    config_keys: {
      commit: { values: ["a", "b"] },
      mode: { values: ["fast", "safe"] },
    },
    benchmarks: [
      { name: "bench-a", result_count: 2 },
      { name: "bench-b", result_count: 2 },
    ],
    timeline_key: "commit",
  })
}

function renderWithMemoryRouter(ui: ReactElement, initialEntries: string[]) {
  return renderWithQueryClient(<MemoryRouter initialEntries={initialEntries}>{ui}</MemoryRouter>)
}

const urlParamHistory: string[] = []

function UrlParamHarness({ replace = true }: { replace?: boolean }) {
  const [value, setValue] = useUrlParam("compare", "commit", ["commit", "mode"], replace)
  const location = useLocation()
  const navigationType = useNavigationType()

  urlParamHistory.push(value)

  return (
    <>
      <div data-testid="value">{value}</div>
      <div data-testid="location">{location.pathname + location.search}</div>
      <div data-testid="navigation-type">{navigationType}</div>
      <button type="button" onClick={() => setValue("mode")}>
        Set mode
      </button>
      <button type="button" onClick={() => setValue("commit")}>
        Set default
      </button>
    </>
  )
}

function SetUrlParamsHarness() {
  const setUrlParams = useSetUrlParams()
  const location = useLocation()

  return (
    <>
      <div data-testid="location">{location.pathname + location.search}</div>
      <button type="button" onClick={() => setUrlParams({ compare: "mode", threshold: "20", bench: null, empty: "" })}>
        Apply updates
      </button>
    </>
  )
}

const filtersHistory: Array<Record<string, string>> = []

function UrlFiltersHarness() {
  const { host, bench } = useUrlHostBenchmark()
  const { filters, setFilter, clearFilters } = useUrlFilters()
  const location = useLocation()

  filtersHistory.push({ ...filters })

  return (
    <>
      <div data-testid="host">{host}</div>
      <div data-testid="bench">{bench}</div>
      <div data-testid="filters">{JSON.stringify(filters)}</div>
      <div data-testid="location">{location.pathname + location.search}</div>
      <button type="button" onClick={() => setFilter("mode", "safe")}>
        Set mode safe
      </button>
      <button type="button" onClick={() => setFilter("commit", "b")}>
        Set commit b
      </button>
      <button type="button" onClick={() => setFilter("mode", "invalid")}>
        Set invalid mode
      </button>
      <button type="button" onClick={() => clearFilters()}>
        Clear filters
      </button>
    </>
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeUrlStateHostIndex(), { host: HOST }))
  urlParamHistory.length = 0
  filtersHistory.length = 0
})

describe("useUrlParam", () => {
  it("sets and clears a query param relative to the default value", async () => {
    const user = userEvent.setup()
    renderWithMemoryRouter(<UrlParamHarness />, ["/timeline?keep=1"])

    expect(await screen.findByTestId("value")).toHaveTextContent("commit")

    await user.click(screen.getByRole("button", { name: "Set mode" }))
    await waitFor(() => {
      expect(screen.getByTestId("value")).toHaveTextContent("mode")
    })

    let locationText = screen.getByTestId("location").textContent ?? ""
    expect(locationText).toContain("keep=1")
    expect(locationText).toContain("compare=mode")

    await user.click(screen.getByRole("button", { name: "Set default" }))
    await waitFor(() => {
      expect(screen.getByTestId("value")).toHaveTextContent("commit")
    })

    locationText = screen.getByTestId("location").textContent ?? ""
    expect(locationText).toContain("keep=1")
    expect(locationText).not.toContain("compare=")
  })

  it("falls back to default and removes invalid param values from the URL", async () => {
    renderWithMemoryRouter(<UrlParamHarness />, ["/timeline?compare=bad"])

    await waitFor(() => {
      expect(screen.getByTestId("location").textContent).not.toContain("compare=")
    })

    expect(urlParamHistory.length).toBeGreaterThan(0)
    for (const value of urlParamHistory) {
      expect(value).toBe("commit")
    }
  })

  it("uses replace navigation when replace=true", async () => {
    const user = userEvent.setup()
    renderWithMemoryRouter(<UrlParamHarness replace={true} />, ["/timeline"])

    expect(await screen.findByTestId("navigation-type")).toHaveTextContent("POP")

    await user.click(screen.getByRole("button", { name: "Set mode" }))
    await waitFor(() => {
      expect(screen.getByTestId("navigation-type")).toHaveTextContent("REPLACE")
    })
  })

  it("uses push navigation when replace=false", async () => {
    const user = userEvent.setup()
    renderWithMemoryRouter(<UrlParamHarness replace={false} />, ["/timeline"])

    expect(await screen.findByTestId("navigation-type")).toHaveTextContent("POP")

    await user.click(screen.getByRole("button", { name: "Set mode" }))
    await waitFor(() => {
      expect(screen.getByTestId("navigation-type")).toHaveTextContent("PUSH")
    })
  })
})

describe("useSetUrlParams", () => {
  it("applies multiple URL updates and removes null/empty params", async () => {
    const user = userEvent.setup()
    renderWithMemoryRouter(<SetUrlParamsHarness />, [`/impact?host=${HOST}&bench=${BENCH}&keep=1`])

    await waitFor(() => {
      const locationText = screen.getByTestId("location").textContent ?? ""
      const params = new URLSearchParams(locationText.split("?")[1] ?? "")
      expect(params.size).toBe(3)
      expect(params.get("host")).toBe(HOST)
      expect(params.get("bench")).toBe(BENCH)
      expect(params.get("keep")).toBe("1")
    })

    await user.click(screen.getByRole("button", { name: "Apply updates" }))

    await waitFor(() => {
      const locationText = screen.getByTestId("location").textContent ?? ""
      const params = new URLSearchParams(locationText.split("?")[1] ?? "")
      expect(params.size).toBe(4)
      expect(params.get("host")).toBe(HOST)
      expect(params.get("keep")).toBe("1")
      expect(params.get("compare")).toBe("mode")
      expect(params.get("threshold")).toBe("20")
      expect(params.has("bench")).toBe(false)
      expect(params.has("empty")).toBe(false)
    })
  })
})

describe("useUrlFilters", () => {
  it("reads valid filters and removes unsupported/invalid f_* params", async () => {
    renderWithRouterAndQueryClient(<UrlFiltersHarness />, {
      initialEntries: [`/timeline?host=${HOST}&bench=${BENCH}&f_mode=fast&f_commit=z&f_unsupported=1`],
    })

    await waitFor(() => {
      const locationText = screen.getByTestId("location").textContent ?? ""
      expect(locationText).not.toContain("f_commit=z")
      expect(locationText).not.toContain("f_unsupported=1")
    })

    // filters must never have exposed invalid values on any render
    expect(filtersHistory.length).toBeGreaterThan(0)
    for (const snapshot of filtersHistory) {
      expect(snapshot).toEqual({ mode: "fast" })
    }
  })

  it("updates and clears f_* filters via setFilter/clearFilters", async () => {
    const user = userEvent.setup()
    renderWithRouterAndQueryClient(<UrlFiltersHarness />, {
      initialEntries: [`/timeline?host=${HOST}&bench=${BENCH}`],
    })

    expect(await screen.findByTestId("filters")).toHaveTextContent("{}")

    await user.click(screen.getByRole("button", { name: "Set mode safe" }))
    await user.click(screen.getByRole("button", { name: "Set commit b" }))

    await waitFor(() => {
      const filters = JSON.parse(screen.getByTestId("filters").textContent ?? "{}")
      expect(filters).toEqual({ mode: "safe", commit: "b" })
    })

    await user.click(screen.getByRole("button", { name: "Set invalid mode" }))
    await waitFor(() => {
      const filters = JSON.parse(screen.getByTestId("filters").textContent ?? "{}")
      expect(filters).toEqual({ commit: "b" })
    })

    await user.click(screen.getByRole("button", { name: "Clear filters" }))
    await waitFor(() => {
      const filters = JSON.parse(screen.getByTestId("filters").textContent ?? "{}")
      expect(filters).toEqual({})
      const locationText = screen.getByTestId("location").textContent ?? ""
      expect(locationText).not.toContain("f_mode=")
      expect(locationText).not.toContain("f_commit=")
    })
  })
})
