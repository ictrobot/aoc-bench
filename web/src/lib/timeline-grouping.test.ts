import { describe, expect, it } from "vitest"
import type { CompactResult } from "./types.ts"
import { expandTimelineGroups, groupTimelineResults } from "./timeline-grouping.ts"

const result = (commit: string, token: number): CompactResult => ({
  bench: "bench",
  config: { commit, build: "release" },
  measurement_token: token,
  mean_ns: 100,
  ci95_half_ns: 2,
})

describe("groupTimelineResults", () => {
  it("merges only contiguous cases with the same positive measurement token", () => {
    const groups = groupTimelineResults([result("a", 1), result("b", 1), result("c", 2), result("d", 0)], "commit", [
      "a",
      "b",
      "c",
      "d",
    ])

    expect(
      groups.map(({ fullValue, caseCount, measurement_token }) => [fullValue, caseCount, measurement_token]),
    ).toEqual([
      ["a–b", 2, 1],
      ["c", 1, 2],
      ["d", 1, 0],
    ])
    expect(groups[0].configs.map((config) => config.commit)).toEqual(["a", "b"])

    const bars = expandTimelineGroups(groups, "commit")
    expect(bars.map(({ axisValue, groupIndex, isRangeStart }) => [axisValue, groupIndex, isRangeStart])).toEqual([
      ["a", 0, true],
      ["b", 0, false],
      ["c", 1, true],
      ["d", 2, true],
    ])
    expect(bars.map((bar) => bar.fullValue)).toEqual(["a–b", "a–b", "c", "d"])
    expect(bars.map((bar) => bar.errorBarCi95HalfNs)).toEqual([2, undefined, 2, 2])
  })

  it("does not merge across gaps or different fixed configs", () => {
    const differentBuild = result("b", 1)
    differentBuild.config.build = "debug"
    expect(groupTimelineResults([result("a", 1), result("c", 1)], "commit", ["a", "b", "c"])).toHaveLength(2)
    expect(groupTimelineResults([result("a", 1), differentBuild], "commit", ["a", "b"])).toHaveLength(2)
  })

  it("merges annotated cases and retains every label", () => {
    const groups = groupTimelineResults([result("a", 1), result("b", 1), result("c", 1)], "commit", ["a", "b", "c"], {
      a: "release",
      c: "tip",
    })

    expect(groups).toHaveLength(1)
    expect(groups[0].annotations).toEqual([
      { value: "a", label: "release" },
      { value: "c", label: "tip" },
    ])
  })
})
