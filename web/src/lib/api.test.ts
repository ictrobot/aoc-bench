import { QueryClient } from "@tanstack/react-query"
import { describe, expect, it, vi } from "vitest"
import { createSnapshotRetry, SnapshotNotFoundError } from "./api.ts"

function makeQueryClient(snapshotId?: string) {
  const qc = new QueryClient()
  if (snapshotId) {
    qc.setQueryData(["index"], { snapshot_id: snapshotId })
  }
  vi.spyOn(qc, "invalidateQueries")
  return qc
}

describe("createSnapshotRetry", () => {
  it("allows 3 retries for non-snapshot errors", () => {
    const qc = makeQueryClient()
    const retry = createSnapshotRetry(qc)

    expect(retry(0, new Error("network error"))).toBe(true)
    expect(retry(1, new Error("network error"))).toBe(true)
    expect(retry(2, new Error("network error"))).toBe(true)
    expect(retry(3, new Error("network error"))).toBe(false)
  })

  it("allows 1 retry for SnapshotNotFoundError", () => {
    const qc = makeQueryClient("snap-1")
    const retry = createSnapshotRetry(qc)

    expect(retry(0, new SnapshotNotFoundError("old/path"))).toBe(true)
    expect(retry(1, new SnapshotNotFoundError("old/path"))).toBe(false)
  })

  it("invalidates index query on first SnapshotNotFoundError", () => {
    const qc = makeQueryClient("snap-1")
    const retry = createSnapshotRetry(qc)

    retry(0, new SnapshotNotFoundError("old/path"))

    expect(qc.invalidateQueries).toHaveBeenCalledWith({ queryKey: ["index"] })
  })

  it("does not invalidate again for the same snapshot ID", () => {
    const qc = makeQueryClient("snap-1")
    const retry = createSnapshotRetry(qc)

    retry(0, new SnapshotNotFoundError("old/path"))
    retry(0, new SnapshotNotFoundError("old/path"))

    expect(qc.invalidateQueries).toHaveBeenCalledTimes(1)
  })

  it("invalidates again when snapshot ID changes", () => {
    const qc = makeQueryClient("snap-1")
    const retry = createSnapshotRetry(qc)

    retry(0, new SnapshotNotFoundError("old/path"))
    expect(qc.invalidateQueries).toHaveBeenCalledTimes(1)

    // Simulate index refetch producing a new snapshot
    qc.setQueryData(["index"], { snapshot_id: "snap-2" })

    retry(0, new SnapshotNotFoundError("another/path"))
    expect(qc.invalidateQueries).toHaveBeenCalledTimes(2)
  })

  it("does not invalidate when no index data is cached", () => {
    const qc = makeQueryClient() // no snapshot in cache
    const retry = createSnapshotRetry(qc)

    retry(0, new SnapshotNotFoundError("old/path"))

    expect(qc.invalidateQueries).not.toHaveBeenCalled()
  })

  it("calls onRecovery callback when invalidating", () => {
    const qc = makeQueryClient("snap-1")
    const onRecovery = vi.fn()
    const retry = createSnapshotRetry(qc, onRecovery)

    retry(0, new SnapshotNotFoundError("old/path"))

    expect(onRecovery).toHaveBeenCalledTimes(1)
  })

  it("does not call onRecovery for duplicate snapshot invalidation", () => {
    const qc = makeQueryClient("snap-1")
    const onRecovery = vi.fn()
    const retry = createSnapshotRetry(qc, onRecovery)

    retry(0, new SnapshotNotFoundError("old/path"))
    retry(0, new SnapshotNotFoundError("old/path"))

    expect(onRecovery).toHaveBeenCalledTimes(1)
  })

  it("does not call onRecovery for non-snapshot errors", () => {
    const qc = makeQueryClient("snap-1")
    const onRecovery = vi.fn()
    const retry = createSnapshotRetry(qc, onRecovery)

    retry(0, new Error("network error"))

    expect(onRecovery).not.toHaveBeenCalled()
  })
})
