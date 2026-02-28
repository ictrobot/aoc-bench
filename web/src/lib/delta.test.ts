import { describe, expect, it } from "vitest"
import { relativeChange } from "./delta.ts"

describe("relativeChange", () => {
  it("returns relative change for positive finite baselines", () => {
    expect(relativeChange(150, 100)).toBe(0.5)
    expect(relativeChange(75, 100)).toBe(-0.25)
  })

  it("returns null for zero or negative baselines", () => {
    expect(relativeChange(100, 0)).toBeNull()
    expect(relativeChange(100, -1)).toBeNull()
  })

  it("returns null for non-finite inputs", () => {
    expect(relativeChange(Number.NaN, 100)).toBeNull()
    expect(relativeChange(100, Number.NaN)).toBeNull()
    expect(relativeChange(100, Number.POSITIVE_INFINITY)).toBeNull()
  })
})
