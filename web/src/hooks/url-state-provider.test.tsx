import { screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter, useLocation } from "react-router"
import { beforeEach, describe, expect, it, vi } from "vitest"
import * as api from "@/lib/api.ts"
import { UrlHostProvider } from "@/hooks/url-state-provider.tsx"
import { useUrlHostBenchmark } from "@/hooks/use-url-state.tsx"
import { makeHostIndex } from "@/test/fixtures.ts"
import { renderWithQueryClient, renderWithRouterAndQueryClient } from "@/test/query-client.tsx"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
}))

const HOST_A = "linux-x64"
const HOST_B = "mac-arm64"
const mockLoadIndex = vi.mocked(api.loadIndex)

function makeIndexWithTwoHosts() {
  return {
    schema_version: 1 as const,
    snapshot_id: "snapshot-a",
    hosts: {
      [HOST_A]: makeHostIndex({
        config_keys: { commit: { values: ["a", "b"] } },
        benchmarks: [
          { name: "bench-a", result_count: 1 },
          { name: "bench-b", result_count: 1 },
        ],
        timeline_key: "commit",
      }),
      [HOST_B]: makeHostIndex({
        config_keys: { commit: { values: ["a", "b"] } },
        benchmarks: [
          { name: "bench-a", result_count: 1 },
          { name: "bench-b", result_count: 1 },
        ],
        timeline_key: "commit",
      }),
    },
  }
}

function UrlHostProbe() {
  const { host, bench, setHost, setBench } = useUrlHostBenchmark()
  const location = useLocation()

  return (
    <>
      <div data-testid="host">{host}</div>
      <div data-testid="bench">{bench}</div>
      <div data-testid="location">{location.pathname + location.search}</div>
      <button type="button" onClick={() => setHost(HOST_B)}>
        Set host B
      </button>
      <button type="button" onClick={() => setBench("bench-b")}>
        Set bench b
      </button>
    </>
  )
}

describe("UrlHostProvider", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLoadIndex.mockResolvedValue(makeIndexWithTwoHosts())
  })

  it("uses fallback host/bench when query params are missing", async () => {
    renderWithRouterAndQueryClient(<UrlHostProbe />, { initialEntries: ["/timeline"] })

    expect(await screen.findByTestId("host")).toHaveTextContent(HOST_A)
    expect(screen.getByTestId("bench")).toHaveTextContent("bench-a")
  })

  it("strips invalid host/bench values while preserving unrelated params", async () => {
    renderWithRouterAndQueryClient(<UrlHostProbe />, {
      initialEntries: ["/timeline?host=unknown&bench=missing&keep=1"],
    })

    expect(await screen.findByTestId("host")).toHaveTextContent(HOST_A)
    expect(screen.getByTestId("bench")).toHaveTextContent("bench-a")

    await waitFor(() => {
      const locationText = screen.getByTestId("location").textContent ?? ""
      expect(locationText).not.toContain("host=unknown")
      expect(locationText).not.toContain("bench=missing")
      expect(locationText).toContain("keep=1")
    })
  })

  it("updates host and bench params through provider setters", async () => {
    const user = userEvent.setup()
    renderWithRouterAndQueryClient(<UrlHostProbe />, {
      initialEntries: [`/timeline?host=${HOST_A}&bench=bench-a&keep=1`],
    })

    expect(await screen.findByTestId("host")).toHaveTextContent(HOST_A)
    await user.click(screen.getByRole("button", { name: "Set host B" }))
    await user.click(screen.getByRole("button", { name: "Set bench b" }))

    await waitFor(() => {
      expect(screen.getByTestId("host")).toHaveTextContent(HOST_B)
      expect(screen.getByTestId("bench")).toHaveTextContent("bench-b")
      const locationText = screen.getByTestId("location").textContent ?? ""
      expect(locationText).toContain(`host=${HOST_B}`)
      expect(locationText).toContain("bench=bench-b")
      expect(locationText).toContain("keep=1")
    })
  })

  it("does not strip valid URL params while the index is loading", async () => {
    let resolveIndex: ((value: ReturnType<typeof makeIndexWithTwoHosts>) => void) | undefined
    const pendingIndex = new Promise<ReturnType<typeof makeIndexWithTwoHosts>>((resolve) => {
      resolveIndex = resolve
    })
    mockLoadIndex.mockReturnValueOnce(pendingIndex)

    renderWithQueryClient(
      <MemoryRouter initialEntries={[`/timeline?host=${HOST_A}&bench=bench-a`]}>
        <UrlHostProvider>
          <UrlHostProbe />
        </UrlHostProvider>
        <div data-testid="tree-rendered" />
      </MemoryRouter>,
    )

    await waitFor(() => {
      expect(screen.getByTestId("tree-rendered")).toBeInTheDocument()
      expect(screen.queryByTestId("host")).not.toBeInTheDocument()
    })

    resolveIndex?.(makeIndexWithTwoHosts())

    await waitFor(() => {
      expect(screen.getByTestId("host")).toHaveTextContent(HOST_A)
      expect(screen.getByTestId("bench")).toHaveTextContent("bench-a")
      const location = screen.getByTestId("location").textContent ?? ""
      expect(location).toContain(`host=${HOST_A}`)
      expect(location).toContain("bench=bench-a")
    })
  })
})
