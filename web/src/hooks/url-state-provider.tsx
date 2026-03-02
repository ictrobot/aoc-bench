import { type ReactNode, useCallback, useMemo } from "react"
import { UrlStateContext, useUrlParam } from "@/hooks/use-url-state.tsx"
import { useIndex } from "@/hooks/queries.ts"

/**
 * Provides validated host and bench URL params to all descendants.
 * Centralises validation useEffects so they run exactly once regardless of
 * how many components call useUrlHostBenchmark().
 *
 * Returns null until the index has loaded and valid host/bench params are
 * resolved, so consumers can assume index, host, and hostIndex are always defined.
 * AppContent already guards loading/error states before mounting this provider.
 */
export function UrlHostProvider({ children }: { children: ReactNode }) {
  const { data: index } = useIndex()

  const hosts = useMemo(() => (index ? Object.keys(index.hosts).sort() : undefined), [index])
  const fallbackHost = hosts?.[0] ?? ""
  const [host, setHost] = useUrlParam("host", fallbackHost, hosts, false)

  const hostIndex = index?.hosts[host]

  const benchmarks = useMemo(() => (hostIndex ? hostIndex.benchmarks.map((b) => b.name) : undefined), [hostIndex])
  const fallbackBench = benchmarks?.[0] ?? ""
  const [bench, setBench] = useUrlParam("bench", fallbackBench, benchmarks, false)

  const validConfigKeyValue = useCallback(
    (key: string, value: string) => !!value && (hostIndex?.config_keys[key]?.values.includes(value) ?? false),
    [hostIndex],
  )

  if (!index || !hostIndex || !bench) return null

  return (
    <UrlStateContext.Provider value={{ index, host, hostIndex, validConfigKeyValue, setHost, bench, setBench }}>
      {children}
    </UrlStateContext.Provider>
  )
}
