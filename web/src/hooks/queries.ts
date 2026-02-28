import type { CompactResult, HistorySeries, HostIndex, IndexedHistory, IndexedResults } from "@/lib/types.ts"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import * as api from "@/lib/api.ts"
import { buildConfigSignature } from "@/lib/config-signature.ts"

const RAW_QUERY_STALE_TIME = Infinity
const RAW_QUERY_GC_TIME_MS = 30_000
const DECODED_QUERY_STALE_TIME = Infinity
const DECODED_QUERY_GC_TIME_MS = 0
const DECODED_QUERY_RETRY = false

/**
 * Raw indexed data has two access paths:
 * - Observed hooks keep it active while a page is mounted, so changing benchmark/config
 *   within that page can reuse raw data without waiting for GC timers.
 * - Decoded queryFns use imperative `ensureQueryData` calls, where hooks cannot run.
 *   This reuses (or populates) the exact same
 *   raw cache entry keyed by host/snapshot/bench.
 */
function indexedResultsQueryOptions(snapshotId: string | undefined, host: string, hostIndex: HostIndex | undefined) {
  return {
    queryKey: ["results", snapshotId, host] as const,
    queryFn: () => api.loadResults(hostIndex!),
    staleTime: RAW_QUERY_STALE_TIME,
    gcTime: RAW_QUERY_GC_TIME_MS,
  }
}

function indexedHistoryQueryOptions(
  snapshotId: string | undefined,
  host: string,
  bench: string,
  hostIndex: HostIndex | undefined,
) {
  return {
    queryKey: ["history", snapshotId, host, bench] as const,
    queryFn: () => api.loadHistory(hostIndex!, bench),
    staleTime: RAW_QUERY_STALE_TIME,
    gcTime: RAW_QUERY_GC_TIME_MS,
  }
}

/** Load the top-level web export snapshot index. */
export function useIndex() {
  return useQuery({
    queryKey: ["index"] as const,
    queryFn: api.loadIndex,
    staleTime: Infinity,
  })
}

/** Select one host's index entry from the global index query. */
export function useHostIndex(host: string) {
  const { data: index, ...rest } = useIndex()
  return { ...rest, data: index?.hosts[host] }
}

/** Load and decode latest timeline rows embedded in one host index entry. */
export function useLatestResults(host: string) {
  const { data: index } = useIndex()
  const snapshotId = index?.snapshot_id
  const hostIndex = index?.hosts[host]
  const enabled = !!host && !!snapshotId && !!hostIndex

  return useQuery<CompactResult[] | null>({
    queryKey: ["decoded-latest-results", snapshotId, host],
    queryFn: () => api.decodeLatestResults(hostIndex!),
    staleTime: DECODED_QUERY_STALE_TIME,
    gcTime: DECODED_QUERY_GC_TIME_MS,
    retry: DECODED_QUERY_RETRY,
    enabled,
  })
}

/** Load and decode all compact results for a host. */
export function useCompactResults(host: string) {
  const queryClient = useQueryClient()
  const { data: index } = useIndex()
  const hostIndex = index?.hosts[host]
  const snapshotId = index?.snapshot_id
  const enabled = !!hostIndex && !!snapshotId

  // Used to keep the raw indexed results active
  useQuery<IndexedResults>({
    ...indexedResultsQueryOptions(snapshotId, host, hostIndex),
    enabled,
  })

  return useQuery<CompactResult[]>({
    queryKey: ["decoded-results", snapshotId, host],
    queryFn: async () => {
      const data = await queryClient.ensureQueryData<IndexedResults>(
        indexedResultsQueryOptions(snapshotId, host, hostIndex),
      )
      return api.decodeResults(hostIndex!, data)
    },
    staleTime: DECODED_QUERY_STALE_TIME,
    gcTime: DECODED_QUERY_GC_TIME_MS,
    retry: DECODED_QUERY_RETRY,
    enabled,
  })
}

/** Load and decode compact results for a single benchmark on a host. */
export function useBenchmarkResults(host: string, bench: string) {
  const queryClient = useQueryClient()
  const { data: index } = useIndex()
  const hostIndex = index?.hosts[host]
  const snapshotId = index?.snapshot_id
  const enabled = !!hostIndex && !!snapshotId && !!bench

  // Used to keep the raw indexed results active
  useQuery<IndexedResults>({
    ...indexedResultsQueryOptions(snapshotId, host, hostIndex),
    enabled,
  })

  return useQuery<CompactResult[]>({
    queryKey: ["decoded-benchmark-results", snapshotId, host, bench],
    queryFn: async () => {
      const data = await queryClient.ensureQueryData<IndexedResults>(
        indexedResultsQueryOptions(snapshotId, host, hostIndex),
      )
      return api.decodeResultsForBenchmark(hostIndex!, data, bench)
    },
    staleTime: DECODED_QUERY_STALE_TIME,
    gcTime: DECODED_QUERY_GC_TIME_MS,
    retry: DECODED_QUERY_RETRY,
    enabled,
  })
}

/** Load and decode historical series for one benchmark+config tuple. */
export function useBenchmarkHistory(host: string, bench: string, config: Record<string, string>) {
  const queryClient = useQueryClient()
  const { data: index } = useIndex()
  const hostIndex = index?.hosts[host]
  const snapshotId = index?.snapshot_id
  const enabled = !!hostIndex && !!snapshotId && !!bench

  // Used to keep the raw indexed history active
  useQuery<IndexedHistory>({
    ...indexedHistoryQueryOptions(snapshotId, host, bench, hostIndex),
    enabled,
  })

  return useQuery<HistorySeries[]>({
    queryKey: ["decoded-history", snapshotId, host, bench, buildConfigSignature(config, "")],
    queryFn: async () => {
      const data = await queryClient.ensureQueryData<IndexedHistory>(
        indexedHistoryQueryOptions(snapshotId, host, bench, hostIndex),
      )
      return api.decodeHistory(hostIndex!, data, config)
    },
    staleTime: DECODED_QUERY_STALE_TIME,
    gcTime: DECODED_QUERY_GC_TIME_MS,
    retry: DECODED_QUERY_RETRY,
    enabled,
  })
}
