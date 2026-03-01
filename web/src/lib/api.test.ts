import { QueryClient } from "@tanstack/react-query"
import { afterEach, describe, expect, it, vi } from "vitest"
import type { HostIndex } from "./types.ts"
import { createSnapshotRetry, loadHistory, loadResults, SnapshotNotFoundError } from "./api.ts"

function makeQueryClient(snapshotId?: string) {
  const qc = new QueryClient()
  if (snapshotId) {
    qc.setQueryData(["index"], { snapshot_id: snapshotId })
  }
  vi.spyOn(qc, "invalidateQueries")
  return qc
}

function makeSnapshotHostIndex(): HostIndex {
  return {
    last_updated: 1_700_000_000,
    config_keys: {},
    benchmarks: [],
    timeline_key: "compiler",
    results_path: "snapshots/snap-1/linux-x64/results.json",
    history_dir: "snapshots/snap-1/linux-x64/history",
  }
}

afterEach(() => {
  vi.restoreAllMocks()
})

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

describe("fetchJson snapshot handling", () => {
  it("throws SnapshotNotFoundError for snapshot 404 responses", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 404, statusText: "Not Found" }))

    await expect(loadResults(makeSnapshotHostIndex())).rejects.toBeInstanceOf(SnapshotNotFoundError)
  })

  it("throws SnapshotNotFoundError for SPA HTML fallback responses", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("<!doctype html><html><body>App shell</body></html>", {
        status: 200,
        statusText: "OK",
        headers: { "content-type": "text/html; charset=utf-8" },
      }),
    )

    await expect(loadHistory(makeSnapshotHostIndex(), "bench-a")).rejects.toBeInstanceOf(SnapshotNotFoundError)
  })

  it("keeps malformed non-HTML snapshot JSON as a parse error", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("{not-json", {
        status: 200,
        statusText: "OK",
        headers: { "content-type": "application/json" },
      }),
    )

    const error = await loadResults(makeSnapshotHostIndex()).catch((err: unknown) => err)
    expect(error).not.toBeInstanceOf(SnapshotNotFoundError)
    expect(error).toBeInstanceOf(Error)
  })
})
