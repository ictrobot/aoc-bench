import { describe, expect, it } from "vitest"
import { buildDashboardInsightsData, type DashboardInsightEntry } from "./dashboard-insights.ts"

function makeEntry(name: string, mean_ns: number): DashboardInsightEntry {
  return { name, mean_ns, href: `/benchmark?bench=${name}` }
}

describe("buildDashboardInsightsData", () => {
  it("groups slices as top 10 plus other", () => {
    const entries = Array.from({ length: 12 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    const data = buildDashboardInsightsData(entries)

    expect(data.slices).toHaveLength(11)
    expect(data.slices.slice(0, 10).map((slice) => slice.name)).toEqual([
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
    ])
    expect(data.slices[10]).toMatchObject({
      name: "Other",
      mean_ns: 3,
      benchmarkCount: 2,
    })
  })

  it("omits the other slice when there are 10 or fewer benchmarks", () => {
    const entries = Array.from({ length: 10 }, (_, i) => makeEntry(`bench-${i + 1}`, i + 1))

    const data = buildDashboardInsightsData(entries)

    expect(data.slices).toHaveLength(10)
    expect(data.slices.map((slice) => slice.name)).not.toContain("Other")
  })

  it("computes shares from the same total used by the headline", () => {
    const data = buildDashboardInsightsData([
      makeEntry("bench-a", 50),
      makeEntry("bench-b", 30),
      makeEntry("bench-c", 20),
    ])

    expect(data.totalNs).toBe(100)
    expect(data.slices).toEqual([
      expect.objectContaining({ name: "bench-a", share: 0.5 }),
      expect.objectContaining({ name: "bench-b", share: 0.3 }),
      expect.objectContaining({ name: "bench-c", share: 0.2 }),
    ])
  })
})
