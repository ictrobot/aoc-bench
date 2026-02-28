/** Build a route path with only non-empty query params included. */
export function withQuery(pathname: string, params: Record<string, string | null | undefined>): string {
  const searchParams = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      searchParams.set(key, value)
    }
  }
  const query = searchParams.toString()
  return query ? `${pathname}?${query}` : pathname
}
