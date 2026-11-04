import { describe, expect, it } from "vitest"
import type { CompactResult } from "./types.ts"
import { groupTimelineResults } from "./timeline-grouping.ts"

const result = (commit: string, token: number): CompactResult => ({
  bench: "bench",
  config: { commit, build: "release" },
  measurement_token: token,
  mean_ns: 100,
  ci95_half_ns: 2,
  run_count: 3,
  multimodal: false,
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
  })

  it("does not merge across gaps or different fixed configs", () => {
    const differentBuild = result("b", 1)
    differentBuild.config.build = "debug"
    expect(groupTimelineResults([result("a", 1), result("c", 1)], "commit", ["a", "b", "c"])).toHaveLength(2)
    expect(groupTimelineResults([result("a", 1), differentBuild], "commit", ["a", "b"])).toHaveLength(2)
  })

  it("does not merge cases whose multimodal flag or run count differ", () => {
    const multimodal = result("b", 1)
    multimodal.multimodal = true
    const fewerRuns = result("b", 1)
    fewerRuns.run_count = 1
    expect(groupTimelineResults([result("a", 1), multimodal], "commit", ["a", "b"])).toHaveLength(2)
    expect(groupTimelineResults([result("a", 1), fewerRuns], "commit", ["a", "b"])).toHaveLength(2)

    const groups = groupTimelineResults(
      [multimodal, { ...multimodal, config: { ...multimodal.config, commit: "c" } }],
      "commit",
      ["a", "b", "c"],
    )
    expect(groups).toHaveLength(1)
    expect(groups[0].multimodal).toBe(true)
  })

  it("does not merge a missing interval with a zero-width one", () => {
    const noInterval = result("a", 1)
    noInterval.ci95_half_ns = null
    const zeroWidth = result("b", 1)
    zeroWidth.ci95_half_ns = 0
    expect(groupTimelineResults([noInterval, zeroWidth], "commit", ["a", "b"])).toHaveLength(2)

    const alsoNoInterval = result("b", 1)
    alsoNoInterval.ci95_half_ns = null
    const groups = groupTimelineResults([noInterval, alsoNoInterval], "commit", ["a", "b"])
    expect(groups).toHaveLength(1)
    expect(groups[0].ci95_half_ns).toBeNull()
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
