import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { act, renderHook, waitFor } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { ReactNode } from "react"
import type { GlobalIndex, HostIndex, IndexedHistory, IndexedResults } from "@/lib/types.ts"
import { useBenchmarkHistory, useBenchmarkResults } from "@/hooks/queries.ts"
import * as api from "@/lib/api.ts"

vi.mock("@/lib/api.ts", () => ({
  loadIndex: vi.fn(),
  loadResults: vi.fn(),
  loadHistory: vi.fn(),
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
const mockDecodeHistory = vi.mocked(api.decodeHistory)

function makeHostIndex(resultsPath: string): HostIndex {
  return {
    last_updated: 1_700_000_000,
    config_keys: {
      compiler: { values: ["stable"] },
      input: { values: ["small"] },
    },
    benchmarks: [
      { name: BENCH, result_count: 1 },
      { name: BENCH_2, result_count: 1 },
    ],
    timeline_key: "compiler",
    results_path: resultsPath,
    history_dir: "history",
  }
}

function makeIndex(snapshotId: string, hostIndex: HostIndex): GlobalIndex {
  return {
    schema_version: 1,
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
  mockLoadHistory.mockResolvedValue({ series: [] } satisfies IndexedHistory)
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
    const rawData: IndexedResults = { results: [[0, 0, 100, 5]] }
    const decoded = [
      {
        bench: BENCH,
        config: { compiler: "stable", input: "small" },
        mean_ns: 100,
        ci95_half_ns: 5,
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
    const rawDataA: IndexedResults = { results: [[0, 0, 111, 3]] }
    const rawDataB: IndexedResults = { results: [[0, 0, 222, 7]] }

    mockLoadIndex.mockResolvedValueOnce(indexA).mockResolvedValueOnce(indexB)
    mockLoadResults.mockResolvedValueOnce(rawDataA).mockResolvedValueOnce(rawDataB)
    mockDecodeResultsForBenchmark.mockImplementation((index, data, bench) => [
      {
        bench,
        config: { source: index.results_path },
        mean_ns: data.results[0][2],
        ci95_half_ns: data.results[0][3],
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
    const rawData: IndexedResults = { results: [[0, 0, 333, 2]] }
    const decoded = [
      {
        bench: BENCH,
        config: { compiler: "stable", input: "small" },
        mean_ns: 333,
        ci95_half_ns: 2,
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
    const rawData: IndexedResults = { results: [[0, 0, 100, 5]] }

    mockLoadIndex.mockResolvedValue(index)
    mockLoadResults.mockResolvedValue(rawData)
    mockDecodeResultsForBenchmark.mockImplementation((_index, _data, bench) => [
      {
        bench,
        config: { compiler: "stable", input: "small" },
        mean_ns: bench === BENCH ? 100 : 200,
        ci95_half_ns: 5,
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
    const rawHistory: IndexedHistory = { series: [[0, 1_000, 100, 5, 10]] }

    const configA = { compiler: "stable", input: "small" }
    const configB = { compiler: "stable", input: "large" }
    const configBReordered = { input: "large", compiler: "stable" }

    mockLoadIndex.mockResolvedValue(index)
    mockLoadHistory.mockResolvedValue(rawHistory)
    mockDecodeHistory.mockImplementation((_index, _data, config) => [
      {
        config: { ...config },
        timestamp: config.input === "small" ? 1_000 : 2_000,
        median_run_mean_ns: config.input === "small" ? 100 : 200,
        median_run_ci95_half_ns: 5,
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
    expect(mockDecodeHistory.mock.calls.map((call) => call[2])).toEqual([configA, configB])
  })
})
