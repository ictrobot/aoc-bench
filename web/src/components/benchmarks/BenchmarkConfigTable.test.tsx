import { render, screen, within, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router"
import { describe, expect, it } from "vitest"
import {
  BenchmarkConfigTable,
  type BenchmarkConfigMetricColumn,
  type BenchmarkConfigTableRow,
} from "./BenchmarkConfigTable.tsx"

interface TestRow extends BenchmarkConfigTableRow {
  meanNs: number
}

const configKeys = {
  build: { values: ["safe", "fast"] },
  threads: { values: ["1", "2"] },
}

const metricColumns: BenchmarkConfigMetricColumn<TestRow>[] = [
  {
    key: "time",
    header: "Time",
    compare: (a, b) => a.meanNs - b.meanNs,
    render: (row) => `${row.meanNs} ns`,
  },
]

function benchmarkOrder(table: HTMLElement) {
  return within(table)
    .getAllByRole("link")
    .map((link) => link.textContent)
}

describe("BenchmarkConfigTable", () => {
  it("resets to index order when the active config sort column disappears", async () => {
    const user = userEvent.setup()
    const rows: TestRow[] = [
      {
        key: "bench-c",
        benchmark: "bench-c",
        benchmarkHref: "/benchmark?bench=bench-c",
        config: { build: "safe", threads: "2" },
        meanNs: 70,
      },
      {
        key: "bench-b",
        benchmark: "bench-b",
        benchmarkHref: "/benchmark?bench=bench-b",
        config: { build: "safe", threads: "1" },
        meanNs: 60,
      },
      {
        key: "bench-a",
        benchmark: "bench-a",
        benchmarkHref: "/benchmark?bench=bench-a",
        config: { build: "fast", threads: "2" },
        meanNs: 50,
      },
    ]

    const { rerender } = render(
      <MemoryRouter>
        <BenchmarkConfigTable
          rows={rows}
          configKeys={configKeys}
          configColumnKeys={["build", "threads"]}
          metricColumns={metricColumns}
        />
      </MemoryRouter>,
    )

    const table = screen.getByRole("table")
    expect(benchmarkOrder(table)).toEqual(["bench-c", "bench-b", "bench-a"])

    await user.click(within(table).getByRole("button", { name: /build/ }))
    expect(benchmarkOrder(table)).toEqual(["bench-b", "bench-c", "bench-a"])

    rerender(
      <MemoryRouter>
        <BenchmarkConfigTable
          rows={rows.filter((row) => row.config.build === "safe")}
          configKeys={configKeys}
          configColumnKeys={["threads"]}
          metricColumns={metricColumns}
        />
      </MemoryRouter>,
    )

    await waitFor(() => {
      expect(benchmarkOrder(screen.getByRole("table"))).toEqual(["bench-c", "bench-b"])
    })
    expect(screen.queryByRole("columnheader", { name: /build/ })).toBeNull()
  })
})
