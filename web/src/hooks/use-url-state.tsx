import { createContext, useCallback, useContext, useEffect, useMemo } from "react"
import { useSearchParams } from "react-router"
import type { GlobalIndex, HostIndex } from "@/lib/types.ts"

const FILTER_PREFIX = "f_"

/**
 * Atomically set multiple URL query params in one replace navigation.
 * A null or empty string value removes the param.
 */
export function useSetUrlParams(): (updates: Record<string, string | null>) => void {
  const [, setSearchParams] = useSearchParams()
  return useCallback(
    (updates: Record<string, string | null>) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          for (const [key, value] of Object.entries(updates)) {
            if (value) {
              next.set(key, value)
            } else {
              next.delete(key)
            }
          }
          return next
        },
        { replace: true },
      )
    },
    [setSearchParams],
  )
}

/**
 * Read and write a single URL query param.
 * Setting to empty string or the defaultValue removes the param from the URL.
 *
 * If validate is provided and the current value fails validation, the hook
 * returns defaultValue and uses a replace navigation to remove the invalid
 * param.
 *
 * validate may be a string[] (checked with .includes) or a stable function
 * (useCallback or module-level). The array reference must be stable too
 * (e.g. from useMemo).
 */
export function useUrlParam(
  name: string,
  defaultValue = "",
  validate?: ((v: string) => boolean) | string[],
  replace = true,
): [string, (v: string) => void] {
  const [searchParams, setSearchParams] = useSearchParams()
  const rawValue = searchParams.get(name) ?? defaultValue
  const isValid = rawValue == defaultValue || checkValid(rawValue, validate)
  const value = isValid ? rawValue : defaultValue

  useEffect(() => {
    if (!validate) return
    if (!checkValid(rawValue, validate) && searchParams.has(name)) {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          next.delete(name)
          return next
        },
        { replace: true },
      )
    }
  }, [rawValue, name, setSearchParams, searchParams, validate])

  const setValue = useCallback(
    (v: string) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          if (v && v !== defaultValue) {
            next.set(name, v)
          } else {
            next.delete(name)
          }
          return next
        },
        { replace },
      )
    },
    [name, defaultValue, replace, setSearchParams],
  )

  return [value, setValue]
}

function checkValid(v: string, validate?: ((v: string) => boolean) | string[]): boolean {
  return !validate || (Array.isArray(validate) ? validate.includes(v) : validate(v))
}

/**
 * Read and write all f_* URL query params as a Record<string, string>.
 * Uses replace: true so filter tweaks don't create new history entries.
 */
export function useUrlFilters(): {
  filters: Record<string, string>
  setFilter: (key: string, value: string) => void
  clearFilters: () => void
} {
  const [searchParams, setSearchParams] = useSearchParams()
  const { validConfigKeyValue } = useUrlHostBenchmark()

  useEffect(() => {
    let hasInvalidFilters = false
    for (const [key, value] of searchParams.entries()) {
      if (!key.startsWith(FILTER_PREFIX)) continue
      const filterKey = key.slice(FILTER_PREFIX.length)
      if (!validConfigKeyValue(filterKey, value)) {
        hasInvalidFilters = true
        break
      }
    }
    if (!hasInvalidFilters) return

    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev)
        for (const [key, value] of [...next.entries()]) {
          if (!key.startsWith(FILTER_PREFIX)) continue
          const filterKey = key.slice(FILTER_PREFIX.length)
          if (!validConfigKeyValue(filterKey, value)) {
            next.delete(key)
          }
        }
        return next
      },
      { replace: true },
    )
  }, [searchParams, setSearchParams, validConfigKeyValue])

  const filters = useMemo((): Record<string, string> => {
    const out: Record<string, string> = {}
    for (const [k, v] of searchParams.entries()) {
      if (k.startsWith(FILTER_PREFIX) && v) {
        const key = k.slice(FILTER_PREFIX.length)
        if (!validConfigKeyValue(key, v)) continue
        out[key] = v
      }
    }
    return out
  }, [searchParams, validConfigKeyValue])

  const setFilter = useCallback(
    (key: string, value: string) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          const paramName = `${FILTER_PREFIX}${key}`
          if (validConfigKeyValue(key, value)) {
            next.set(paramName, value)
          } else {
            next.delete(paramName)
          }
          return next
        },
        { replace: true },
      )
    },
    [setSearchParams, validConfigKeyValue],
  )

  const clearFilters = useCallback(() => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev)
        for (const key of [...next.keys()]) {
          if (key.startsWith(FILTER_PREFIX)) next.delete(key)
        }
        return next
      },
      { replace: true },
    )
  }, [setSearchParams])

  return { filters, setFilter, clearFilters }
}

interface UrlStateContextValue {
  index: GlobalIndex
  host: string
  hostIndex: HostIndex
  validConfigKeyValue: (key: string, value: string) => boolean
  setHost: (host: string) => void
  bench: string
  setBench: (bench: string) => void
}

export const UrlStateContext = createContext<UrlStateContextValue | null>(null)

export function useUrlHostBenchmark() {
  const ctx = useContext(UrlStateContext)
  if (!ctx) throw new Error("useUrlHostBenchmark must be used within UrlHostProvider")
  return ctx
}
