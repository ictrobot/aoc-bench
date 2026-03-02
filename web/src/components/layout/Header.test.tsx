import { screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { useLocation } from "react-router"
import { beforeEach, describe, expect, it, vi } from "vitest"
import { Header } from "./Header.tsx"
import * as api from "@/lib/api.ts"
import { renderWithRouterAndQueryClient } from "@/test/query-client.tsx"
import { TEST_HOST, makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
}))

vi.mock("@/hooks/use-theme.ts", () => ({
  useTheme: () => ({
    isDark: false,
    toggle: vi.fn(),
  }),
}))

vi.mock("@/components/ui/combobox.tsx", () => ({
  Combobox: ({
    value,
    onChange,
    options,
    ariaLabel,
  }: {
    value: string
    onChange: (value: string) => void
    options: { value: string; label: string }[]
    ariaLabel?: string
  }) => (
    <select aria-label={ariaLabel} value={value} onChange={(e) => onChange(e.target.value)}>
      <option value="">Select</option>
      {options.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  ),
}))

const HOST = TEST_HOST
const mockLoadIndex = vi.mocked(api.loadIndex)

function LocationProbe() {
  const location = useLocation()
  return <div data-testid="location">{`${location.pathname}${location.search}`}</div>
}

function makeHeaderHostIndex() {
  return makeHostIndex({
    config_keys: {},
    benchmarks: [
      { name: "bench-a", result_count: 1 },
      { name: "bench-b", result_count: 1 },
    ],
  })
}

describe("Header", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeHeaderHostIndex()))
  })

  it("preserves existing query params when changing host", async () => {
    const user = userEvent.setup()
    mockLoadIndex.mockResolvedValue({
      schema_version: 1,
      snapshot_id: "snapshot-a",
      hosts: {
        "linux-x64": makeHeaderHostIndex(),
        "mac-arm64": makeHeaderHostIndex(),
      },
    })
    renderWithRouterAndQueryClient(
      <>
        <Header />
        <LocationProbe />
      </>,
      { initialEntries: ["/benchmark?host=linux-x64&bench=bench-a"] },
    )

    expect(await screen.findByRole("option", { name: "bench-a" })).toBeInTheDocument()
    await user.selectOptions(screen.getByLabelText("Select host"), "mac-arm64")

    const locationText = screen.getByTestId("location").textContent ?? ""
    expect(locationText).toContain("/benchmark")
    expect(locationText).toContain("host=mac-arm64")
    expect(locationText).toContain("bench=bench-a")
  })

  it("navigates to benchmark route when benchmark search is used", async () => {
    const user = userEvent.setup()
    renderWithRouterAndQueryClient(
      <>
        <Header />
        <LocationProbe />
      </>,
      { initialEntries: [`/?host=${HOST}`] },
    )

    expect(await screen.findByRole("option", { name: "bench-b" })).toBeInTheDocument()
    await user.selectOptions(screen.getByLabelText("Search benchmarks"), "bench-b")

    const locationText = screen.getByTestId("location").textContent ?? ""
    expect(locationText).toContain("/benchmark")
    expect(locationText).toContain("host=linux-x64")
    expect(locationText).toContain("bench=bench-b")
  })
})
