import { describe, expect, it } from "vitest"
import { formatCi } from "./format.ts"

describe("formatCi", () => {
  it("renders a ± half-width", () => {
    expect(formatCi(1500)).toBe("\u00b11.50 \u00b5s")
    expect(formatCi(12)).toBe("\u00b112 ns")
  })

  it("renders a null half-width as no interval", () => {
    // The exporter writes null for a single run and for legacy measurements
    expect(formatCi(null)).toBe("no interval")
  })

  it("renders a genuine zero half-width as ±0 ns", () => {
    expect(formatCi(0)).toBe("\u00b10 ns")
  })
})
