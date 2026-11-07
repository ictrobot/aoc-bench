import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { act, renderHook, waitFor } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { ReactNode } from "react"
import type { GlobalIndex, HistoryRow, HostIndex, ResultRow } from "@/lib/types.ts"
import { useBenchmarkHistory, useBenchmarkResults } from "@/hooks/queries.ts"
import * as api from "@/lib/api.ts"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
  loadResults: vi.fn(),
  loadHistory: vi.fn(),
  historyPart: vi.fn(),
  decodeResultsForBenchmark: vi.fn(),
  decodeHistory: vi.fn(),
}))

const HOST = "linux-x64"
const BENCH = "aoc/bench-1"
const BENCH_2 = "aoc/bench-2"

const mockLoadIndex = vi.mocked(api.loadIndex)
const mockLoadResults = vi.mocked(api.loadResults)
const mockDecodeResultsForBenchmark = vi.mocked(api.decodeResultsForBenchmark)
const mockLoadHistory = vi.mocked(api.loadHistory)
const mockHistoryPart = vi.mocked(api.historyPart)
const mockDecodeHistory = vi.mocked(api.decodeHistory)

function makeHostIndex(resultsPath: string): HostIndex {
  return {
    last_updated: 1_700_000_000,
    config_keys: {
      compiler: { values: ["stable"] },
      input: { values: ["small"] },
    },
    benchmarks: [
      { name: BENCH, result_count: 1, config_keys: [0, 1], history_parts: [0] },
      { name: BENCH_2, result_count: 1, config_keys: [0, 1], history_parts: [0] },
    ],
    timeline_key: "compiler",
    results_path: resultsPath,
    history_dir: "history",
  }
}

function makeIndex(snapshotId: string, hostIndex: HostIndex): GlobalIndex {
  return {
    schema_version: 5,
    snapshot_id: snapshotId,
    hosts: { [HOST]: hostIndex },
  }
}

function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: 1,
        retryDelay: 0,
      },
    },
  })
}

function createWrapper(queryClient: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  }
}

beforeEach(() => {
  vi.clearAllMocks()
  mockLoadHistory.mockResolvedValue([])
  mockHistoryPart.mockReturnValue(0)
  mockDecodeHistory.mockReturnValue([])
})

afterEach(() => {
  vi.useRealTimers()
})

describe("useBenchmarkResults", () => {
  it("retries the raw indexed fetch and decodes once after success", async () => {
    const queryClient = createQueryClient()
    const hostIndex = makeHostIndex("results-a.json")
    const index = makeIndex("snapshot-a", hostIndex)
    const rawData: ResultRow[] = [[0, 0, 0, 100, 5, 3]]
    const decoded = [
      {
        bench: BENCH,
        config: { compiler: "stable", input: "small" },
        measurement_token: 0,
        mean_ns: 100,
        ci95_half_ns: 5,
        run_count: 3,
      },
    ]

    mockLoadIndex.mockResolvedValue(index)
    mockLoadResults.mockRejectedValueOnce(new Error("transient read error")).mockResolvedValue(rawData)
    mockDecodeResultsForBenchmark.mockReturnValue(decoded)

    const { result } = renderHook(() => useBenchmarkResults(HOST, BENCH), {
      wrapper: createWrapper(queryClient),
    })

    await waitFor(() => expect(result.current.isSuccess).toBe(true))

    expect(result.current.data).toEqual(decoded)
    expect(mockLoadResults).toHaveBeenCalledTimes(2)
    expect(mockDecodeResultsForBenchmark).toHaveBeenCalledTimes(1)
    expect(mockDecodeResultsForBenchmark).toHaveBeenCalledWith(hostIndex, rawData, BENCH)
  })

  it("rolls over to a new snapshot key and reloads benchmark data", async () => {
    const queryClient = createQueryClient()
    const hostIndexA = makeHostIndex("results-a.json")
    const hostIndexB = makeHostIndex("results-b.json")
    const indexA = makeIndex("snapshot-a", hostIndexA)
    const indexB = makeIndex("snapshot-b", hostIndexB)
    const rawDataA: ResultRow[] = [[0, 0, 0, 111, 3, 3]]
    const rawDataB: ResultRow[] = [[0, 0, 0, 222, 7, 3]]

    mockLoadIndex.mockResolvedValueOnce(indexA).mockResolvedValueOnce(indexB)
    mockLoadResults.mockResolvedValueOnce(rawDataA).mockResolvedValueOnce(rawDataB)
    mockDecodeResultsForBenchmark.mockImplementation((index, data, bench) => [
      {
        bench,
        config: { source: index.results_path },
        measurement_token: 0,
        mean_ns: data[0][3],
        ci95_half_ns: data[0][4],
        run_count: 3,
      },
    ])

    const { result } = renderHook(() => useBenchmarkResults(HOST, BENCH), {
      wrapper: createWrapper(queryClient),
    })

    await waitFor(() => expect(result.current.data?.[0].mean_ns).toBe(111))

    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: ["index"] })
    })

    await waitFor(() => expect(result.current.data?.[0].mean_ns).toBe(222))

    expect(mockLoadIndex).toHaveBeenCalledTimes(2)
    expect(mockLoadResults).toHaveBeenCalledTimes(2)
    expect(mockLoadResults.mock.calls.map(([arg]) => arg.results_path)).toEqual(["results-a.json", "results-b.json"])
    expect(queryClient.getQueryData(["decoded-benchmark-results", "snapshot-b", HOST, BENCH])).toBeDefined()
  })

  it("keeps raw indexed data while mounted and GC's it 30s after unmount", async () => {
    const queryClient = createQueryClient()
    const hostIndex = makeHostIndex("results-a.json")
    const index = makeIndex("snapshot-a", hostIndex)
    const rawData: ResultRow[] = [[0, 0, 0, 333, 2, 3]]
    const decoded = [
      {
        bench: BENCH,
        config: { compiler: "stable", input: "small" },
        measurement_token: 0,
        mean_ns: 333,
        ci95_half_ns: 2,
        run_count: 3,
      },
    ]

    mockLoadIndex.mockResolvedValue(index)
    mockLoadResults.mockResolvedValue(rawData)
    mockDecodeResultsForBenchmark.mockReturnValue(decoded)

    const { result, unmount } = renderHook(() => useBenchmarkResults(HOST, BENCH), {
      wrapper: createWrapper(queryClient),
    })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))

    const rawKey = ["results", "snapshot-a", HOST] as const
    const decodedKey = ["decoded-benchmark-results", "snapshot-a", HOST, BENCH] as const
    const rawQuery = queryClient.getQueryCache().find({ queryKey: rawKey })

    expect(rawQuery?.getObserversCount()).toBeGreaterThan(0)
    expect(queryClient.getQueryData(rawKey)).toEqual(rawData)
    expect(queryClient.getQueryData(decodedKey)).toEqual(decoded)

    vi.useFakeTimers()
    unmount()

    expect(rawQuery?.getObserversCount()).toBe(0)

    await act(async () => {
      vi.advanceTimersByTime(0)
    })
    expect(queryClient.getQueryData(decodedKey)).toBeUndefined()

    await act(async () => {
      vi.advanceTimersByTime(29_000)
    })
    expect(queryClient.getQueryData(rawKey)).toEqual(rawData)

    await act(async () => {
      vi.advanceTimersByTime(1_001)
    })
    expect(queryClient.getQueryData(rawKey)).toBeUndefined()
  })

  it("reuses cached indexed data when switching benchmarks and decodes again without refetch", async () => {
    const queryClient = createQueryClient()
    const hostIndex = makeHostIndex("results-a.json")
    const index = makeIndex("snapshot-a", hostIndex)
    const rawData: ResultRow[] = [[0, 0, 0, 100, 5, 3]]

    mockLoadIndex.mockResolvedValue(index)
    mockLoadResults.mockResolvedValue(rawData)
    mockDecodeResultsForBenchmark.mockImplementation((_index, _data, bench) => [
      {
        bench,
        config: { compiler: "stable", input: "small" },
        measurement_token: 0,
        mean_ns: bench === BENCH ? 100 : 200,
        ci95_half_ns: 5,
        run_count: 3,
      },
    ])

    const { result, rerender } = renderHook(({ bench }) => useBenchmarkResults(HOST, bench), {
      initialProps: { bench: BENCH },
      wrapper: createWrapper(queryClient),
    })

    await waitFor(() => expect(result.current.data?.[0].bench).toBe(BENCH))
    expect(mockLoadResults).toHaveBeenCalledTimes(1)

    rerender({ bench: BENCH_2 })
    await waitFor(() => expect(result.current.data?.[0].bench).toBe(BENCH_2))

    expect(mockLoadResults).toHaveBeenCalledTimes(1)
    expect(mockDecodeResultsForBenchmark).toHaveBeenCalledTimes(2)
    expect(mockDecodeResultsForBenchmark.mock.calls.map((call) => call[2])).toEqual([BENCH, BENCH_2])
  })
})

describe("useBenchmarkHistory", () => {
  it("reuses cached indexed history for config changes and only decodes per unique config key", async () => {
    const queryClient = createQueryClient()
    const hostIndex = makeHostIndex("results-a.json")
    const index = makeIndex("snapshot-a", hostIndex)
    const rawHistory: HistoryRow[] = [[0, 0, 1_000, 100, 5, 10]]

    const configA = { compiler: "stable", input: "small" }
    const configB = { compiler: "stable", input: "large" }
    const configBReordered = { input: "large", compiler: "stable" }

    mockLoadIndex.mockResolvedValue(index)
    mockLoadHistory.mockResolvedValue(rawHistory)
    mockDecodeHistory.mockImplementation((_index, _data, _bench, config) => [
      {
        config: { ...config },
        measurement_token: 0,
        timestamp: config.input === "small" ? 1_000 : 2_000,
        mean_ns: config.input === "small" ? 100 : 200,
        ci95_half_ns: 5,
        run_count: 10,
      },
    ])

    const { result, rerender } = renderHook(({ config }) => useBenchmarkHistory(HOST, BENCH, config), {
      initialProps: { config: configA },
      wrapper: createWrapper(queryClient),
    })

    await waitFor(() => expect(result.current.data?.[0].timestamp).toBe(1_000))
    expect(mockLoadHistory).toHaveBeenCalledTimes(1)
    expect(mockDecodeHistory).toHaveBeenCalledTimes(1)

    rerender({ config: configB })
    await waitFor(() => expect(result.current.data?.[0].timestamp).toBe(2_000))

    expect(mockLoadHistory).toHaveBeenCalledTimes(1)
    expect(mockDecodeHistory).toHaveBeenCalledTimes(2)

    rerender({ config: configBReordered })
    await waitFor(() => expect(result.current.data?.[0].timestamp).toBe(2_000))

    expect(mockLoadHistory).toHaveBeenCalledTimes(1)
    expect(mockDecodeHistory).toHaveBeenCalledTimes(2)
    expect(mockDecodeHistory.mock.calls.map((call) => call[3])).toEqual([configA, configB])
  })

  it("loads a different history part when the config lives in another part", async () => {
    const queryClient = createQueryClient()
    const hostIndex = makeHostIndex("results-a.json")
    const index = makeIndex("snapshot-a", hostIndex)

    const configA = { compiler: "stable", input: "small" }
    const configB = { compiler: "stable", input: "large" }

    mockLoadIndex.mockResolvedValue(index)
    mockHistoryPart.mockImplementation((_index, _bench, config) => (config.input === "small" ? 0 : 1))
    mockLoadHistory.mockImplementation(async (_index, _bench, part) => [[part, 0, 1_000 * (part + 1), 100, 5, 10]])
    mockDecodeHistory.mockImplementation((_index, data, _bench, config) => [
      {
        config: { ...config },
        measurement_token: 0,
        timestamp: data[0][2],
        mean_ns: 100,
        ci95_half_ns: 5,
        run_count: 10,
      },
    ])

    const { result, rerender } = renderHook(({ config }) => useBenchmarkHistory(HOST, BENCH, config), {
      initialProps: { config: configA },
      wrapper: createWrapper(queryClient),
    })

    await waitFor(() => expect(result.current.data?.[0].timestamp).toBe(1_000))
    expect(mockLoadHistory).toHaveBeenCalledTimes(1)
    expect(mockLoadHistory).toHaveBeenLastCalledWith(hostIndex, BENCH, 0)

    rerender({ config: configB })
    await waitFor(() => expect(result.current.data?.[0].timestamp).toBe(2_000))
    expect(mockLoadHistory).toHaveBeenCalledTimes(2)
    expect(mockLoadHistory).toHaveBeenLastCalledWith(hostIndex, BENCH, 1)

    // Switching back reuses the cached first part.
    rerender({ config: configA })
    await waitFor(() => expect(result.current.data?.[0].timestamp).toBe(1_000))
    expect(mockLoadHistory).toHaveBeenCalledTimes(2)
  })

  it("resolves to no history without fetching when the config has no history part", async () => {
    const queryClient = createQueryClient()
    const hostIndex = makeHostIndex("results-a.json")
    mockLoadIndex.mockResolvedValue(makeIndex("snapshot-a", hostIndex))
    mockHistoryPart.mockReturnValue(-1)

    const { result } = renderHook(() => useBenchmarkHistory(HOST, BENCH, { compiler: "unknown", input: "small" }), {
      wrapper: createWrapper(queryClient),
    })

    await waitFor(() => expect(result.current.data).toEqual([]))
    expect(mockLoadHistory).not.toHaveBeenCalled()
    expect(mockDecodeHistory).not.toHaveBeenCalled()
  })

  it("reports a malformed history part table as a query error", async () => {
    const queryClient = createQueryClient()
    const hostIndex = makeHostIndex("results-a.json")
    mockLoadIndex.mockResolvedValue(makeIndex("snapshot-a", hostIndex))
    mockHistoryPart.mockImplementation(() => {
      throw new Error("Invalid history_parts for benchmark 'aoc/bench-1'")
    })

    const { result } = renderHook(() => useBenchmarkHistory(HOST, BENCH, { compiler: "stable", input: "small" }), {
      wrapper: createWrapper(queryClient),
    })

    await waitFor(() => expect(result.current.error?.message).toMatch(/Invalid history_parts/))
    expect(mockLoadHistory).not.toHaveBeenCalled()
  })
})
