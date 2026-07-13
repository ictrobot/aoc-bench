import { describe, expect, it } from "vitest"
import { downsampleSparkValues } from "./downsample.ts"

describe("downsampleSparkValues", () => {
  it("passes short series through unchanged", () => {
    const values = [3, 1, 2]
    expect(downsampleSparkValues(values)).toBe(values)
  })

  it("bounds long series while preserving one-point spikes", () => {
    const values = Array.from({ length: 10_000 }, () => 100)
    values[5_000] = 900
    values[7_777] = 10
    const out = downsampleSparkValues(values)
    expect(out.length).toBeLessThanOrEqual(400)
    expect(out).toContain(900)
    expect(out).toContain(10)
  })

  it("keeps bucket extremes in encounter order", () => {
    const descending = Array.from({ length: 1_000 }, (_, i) => 1_000 - i)
    const out = downsampleSparkValues(descending)
    expect(out).toEqual([...out].sort((a, b) => b - a))

    const ascending = Array.from({ length: 1_000 }, (_, i) => i)
    const outAsc = downsampleSparkValues(ascending)
    expect(outAsc).toEqual([...outAsc].sort((a, b) => a - b))
  })
})
