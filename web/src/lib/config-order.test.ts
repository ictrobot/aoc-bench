import { describe, expect, it } from "vitest"
import { compareConfigsByOrder, compareConfigValueByOrder } from "./config-order.ts"

describe("compareConfigValueByOrder", () => {
  it("uses canonical value order before lexical fallback", () => {
    expect(compareConfigValueByOrder("safe", "fast", ["safe", "fast"])).toBeLessThan(0)
    expect(compareConfigValueByOrder("fast", "safe", ["safe", "fast"])).toBeGreaterThan(0)
    expect(compareConfigValueByOrder("beta", "alpha", [])).toBeGreaterThan(0)
  })

  it("places missing values after defined ones", () => {
    expect(compareConfigValueByOrder(undefined, "safe", ["safe", "fast"])).toBeGreaterThan(0)
    expect(compareConfigValueByOrder("safe", undefined, ["safe", "fast"])).toBeLessThan(0)
    expect(compareConfigValueByOrder(undefined, undefined, ["safe", "fast"])).toBe(0)
  })
})

describe("compareConfigsByOrder", () => {
  it("compares configs using sorted key names and each key's value order", () => {
    const configKeys = {
      build: { values: ["safe", "fast"] },
      threads: { values: ["1", "2"] },
    }

    expect(
      compareConfigsByOrder({ build: "safe", threads: "2" }, { build: "fast", threads: "1" }, configKeys),
    ).toBeLessThan(0)
    expect(
      compareConfigsByOrder({ build: "safe", threads: "2" }, { build: "safe", threads: "1" }, configKeys),
    ).toBeGreaterThan(0)
    expect(compareConfigsByOrder({ build: "safe", threads: "1" }, { build: "safe", threads: "1" }, configKeys)).toBe(0)
  })
})
