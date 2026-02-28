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
  })

  it("renders an explicit error message when history query fails", async () => {
    mockDecodeHistory.mockImplementation(() => {
      throw new Error("history fetch failed")
    })

    renderWithQueryClient(
      <Dialog open>
        <DialogContent>
          <DialogDescription className="sr-only">Test drill-down dialog content</DialogDescription>
          <DrillDown host={HOST} bench={BENCH} config={{ compiler: "stable" }} />
        </DialogContent>
      </Dialog>,
    )

    expect(await screen.findByText(/Error loading history:/)).toBeInTheDocument()
    expect(screen.getByText(/history fetch failed/)).toBeInTheDocument()
  })
})
