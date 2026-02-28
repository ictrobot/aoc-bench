/** Format a duration in nanoseconds to a human-readable string (ns/µs/ms/s) */
export function formatDurationNs(ns: number): string {
  if (ns >= 1_000_000_000) {
    return `${(ns / 1_000_000_000).toFixed(2)} s`
  } else if (ns >= 1_000_000) {
    return `${(ns / 1_000_000).toFixed(2)} ms`
  } else if (ns >= 1_000) {
    return `${(ns / 1_000).toFixed(2)} \u00b5s`
  } else {
    return `${ns.toFixed(0)} ns`
  }
}

/** Format a CI half-width in nanoseconds as ±<duration> */
export function formatCi(ns: number): string {
  return `\u00b1${formatDurationNs(ns)}`
}

/** Format a Unix timestamp (seconds) as a local date string */
export function formatTimestamp(unixSeconds: number): string {
  return new Date(unixSeconds * 1000).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  })
}

/** Format a Unix timestamp (seconds) as a UTC datetime string */
export function formatTimestampUtc(unixSeconds: number): string {
  return new Date(unixSeconds * 1000).toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
    timeZoneName: "short",
  })
}

/** Shorten a config value for display — truncates hex hashes to 7 chars */
export function shortenValue(value: string): string {
  if (/^[0-9a-f]{8,}$/i.test(value)) {
    return value.slice(0, 7)
  }
  return value
}
