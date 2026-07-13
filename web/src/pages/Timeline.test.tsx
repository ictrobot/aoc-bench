import { screen } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"
import { DrillDown } from "./Timeline.tsx"
import { Dialog, DialogContent, DialogDescription } from "@/components/ui/dialog.tsx"
import * as api from "@/lib/api.ts"
import { renderWithQueryClient } from "@/test/query-client.tsx"
import { TEST_HOST, makeGlobalIndex, makeHostIndex } from "@/test/fixtures.ts"
import type { IndexedHistory } from "@/lib/types.ts"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
  loadHistory: vi.fn(),
  decodeHistory: vi.fn(),
}))

const HOST = TEST_HOST
const BENCH = "aoc/bench-1"
const mockLoadIndex = vi.mocked(api.loadIndex)
const mockLoadHistory = vi.mocked(api.loadHistory)
const mockDecodeHistory = vi.mocked(api.decodeHistory)

function makeTimelineHostIndex() {
  return makeHostIndex({
    config_keys: {
      compiler: { values: ["stable"] },
    },
    benchmarks: [{ name: BENCH, result_count: 1 }],
    timeline_key: "compiler",
  })
}

describe("DrillDown", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLoadIndex.mockResolvedValue(makeGlobalIndex(makeTimelineHostIndex()))
    mockLoadHistory.mockResolvedValue({ series: [] } satisfies IndexedHistory)
    mockDecodeHistory.mockReturnValue([])
  })

  it("defaults a merged range to its final config", () => {
    renderWithQueryClient(
      <Dialog open>
        <DialogContent>
          <DialogDescription className="sr-only">Test drill-down dialog content</DialogDescription>
          <DrillDown
            host={HOST}
            bench={BENCH}
            configs={[{ compiler: "stable" }, { compiler: "nightly" }]}
            varyingKey="compiler"
          />
        </DialogContent>
      </Dialog>,
    )

    expect(screen.getByRole("heading", { name: "History: compiler=nightly" })).toBeInTheDocument()
    expect(screen.getByRole("combobox", { name: "compiler:" })).toHaveTextContent("nightly")
  })

  it("preselects the clicked value of a merged range", () => {
    renderWithQueryClient(
      <Dialog open>
        <DialogContent>
          <DialogDescription className="sr-only">Test drill-down dialog content</DialogDescription>
          <DrillDown
            host={HOST}
            bench={BENCH}
            configs={[{ compiler: "stable" }, { compiler: "nightly" }]}
            varyingKey="compiler"
            initialValue="stable"
          />
        </DialogContent>
      </Dialog>,
    )

    expect(screen.getByRole("heading", { name: "History: compiler=stable" })).toBeInTheDocument()
    expect(screen.getByRole("combobox", { name: "compiler:" })).toHaveTextContent("stable")
  })

  it("renders an explicit error message when history query fails", async () => {
    mockDecodeHistory.mockImplementation(() => {
      throw new Error("history fetch failed")
    })

    renderWithQueryClient(
      <Dialog open>
        <DialogContent>
          <DialogDescription className="sr-only">Test drill-down dialog content</DialogDescription>
          <DrillDown host={HOST} bench={BENCH} configs={[{ compiler: "stable" }]} varyingKey="compiler" />
        </DialogContent>
      </Dialog>,
    )

    expect(await screen.findByText(/Error loading history:/)).toBeInTheDocument()
    expect(screen.getByText(/history fetch failed/)).toBeInTheDocument()
  })
})
