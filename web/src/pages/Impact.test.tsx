import { screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { beforeEach, describe, expect, it, vi } from "vitest"
import { Impact } from "./Impact.tsx"
import * as api from "@/lib/api.ts"
import { renderWithRouterAndQueryClient } from "@/test/query-client.tsx"
import { TEST_HOST, makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"
import type { IndexedResults } from "@/lib/types.ts"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
  loadResults: vi.fn(),
  decodeResults: vi.fn(),
}))

vi.mock("@/components/ui/combobox.tsx", () => ({
  Combobox: ({
    id,
    value,
    onChange,
    options,
    placeholder,
    ariaLabel,
    ariaLabelledBy,
  }: {
    id?: string
    value: string
    onChange: (value: string) => void
    options: { value: string; label: string }[]
    placeholder?: string
    ariaLabel?: string
    ariaLabelledBy?: string
  }) => (
    <select
      id={id}
      aria-label={ariaLabel}
      aria-labelledby={ariaLabelledBy}
      value={value}
      onChange={(e) => onChange(e.target.value)}
    >
      <option value="">{placeholder ?? "Select"}</option>
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
const mockLoadResults = vi.mocked(api.loadResults)
const mockDecodeResults = vi.mocked(api.decodeResults)

function makeImpactHostIndex() {
  return makeHostIndex({
    config_keys: {
      commit: { values: ["a", "b"] },
    },
    benchmarks: [{ name: "bench-a", result_count: 2 }],
    timeline_key: "commit",
  })
}

describe("Impact", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLoadResults.mockResolvedValue({ results: [] } satisfies IndexedResults)
  })

  it("excludes zero-baseline comparisons from all counts and avoids Infinity/NaN output", async () => {
    const user = userEvent.setup()
    const hostIndex = makeImpactHostIndex()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeResults.mockReturnValue([
      { bench: "bench-a", config: { commit: "a" }, mean_ns: 0, ci95_half_ns: 0 },
      { bench: "bench-a", config: { commit: "b" }, mean_ns: 10, ci95_half_ns: 0 },
    ])

    renderWithRouterAndQueryClient(<Impact />, { initialEntries: [`/impact?host=${HOST}`] })

    const fromSelect = await screen.findByLabelText("From:")
    const toSelect = screen.getByLabelText("To:")

    await user.selectOptions(fromSelect, "a")
    await user.selectOptions(toSelect, "b")

    expect(await screen.findByText("0 unchanged")).toBeInTheDocument()
    expect(screen.getByText("0 regressions")).toBeInTheDocument()
    expect(screen.getByText("0 improvements")).toBeInTheDocument()
    expect(screen.queryByText("Regressions")).not.toBeInTheDocument()
    expect(screen.queryByText("Improvements")).not.toBeInTheDocument()
    expect(screen.queryByText(/Infinity|NaN/)).not.toBeInTheDocument()
  })

  it("limits 'To' options to values after the selected 'From' value", async () => {
    const user = userEvent.setup()
    const hostIndex = {
      ...makeImpactHostIndex(),
      config_keys: {
        commit: { values: ["a", "b", "c"] },
      },
    }
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(hostIndex))
    mockDecodeResults.mockReturnValue([])

    renderWithRouterAndQueryClient(<Impact />, { initialEntries: [`/impact?host=${HOST}`] })

    const fromSelect = await screen.findByLabelText("From:")

    await user.selectOptions(fromSelect, "b")

    const toSelect = screen.getByLabelText("To:") as HTMLSelectElement
    const optionValues = Array.from(toSelect.options).map((o) => o.value)
    expect(optionValues).toEqual(["", "c"])
  })
})
