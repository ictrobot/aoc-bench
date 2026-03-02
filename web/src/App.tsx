import { lazy, Suspense, Component, type ReactNode, type ErrorInfo } from "react"
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { createSnapshotRetry } from "@/lib/api.ts"
import { ThemeProvider } from "@/hooks/theme-provider.tsx"
import { Header } from "@/components/layout/Header.tsx"
import { Footer } from "@/components/layout/Footer.tsx"
import { Dashboard } from "@/pages/Dashboard.tsx"
import { Impact } from "@/pages/Impact.tsx"
import { BenchmarkDetail } from "@/pages/BenchmarkDetail.tsx"
import { useIndex } from "@/hooks/queries.ts"
import { UrlHostProvider } from "@/hooks/url-state-provider.tsx"

const Timeline = lazy(() => import("@/pages/Timeline.tsx").then((m) => ({ default: m.Timeline })))
const ReactQueryDevtools = import.meta.env.DEV
  ? lazy(() => import("@tanstack/react-query-devtools").then((m) => ({ default: m.ReactQueryDevtools })))
  : null

const queryClient = new QueryClient()
queryClient.setDefaultOptions({
  queries: {
    retry: createSnapshotRetry(queryClient, () => {
      console.info("[aoc-bench] Stale snapshot detected, reloading index")
    }),
  },
})

class ErrorBoundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state = { error: null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("Render error:", error, info)
  }

  render() {
    if (this.state.error) {
      return (
        <div className="flex h-screen items-center justify-center text-destructive">
          Unexpected error: {(this.state.error as Error).message}
        </div>
      )
    }
    return this.props.children
  }
}

function AppContent() {
  const { data: index, isLoading, error } = useIndex()
  const location = useLocation()

  if (isLoading) {
    return <div className="flex h-screen items-center justify-center text-muted-foreground">Loading...</div>
  }

  // export-web guarantees at least one host, and that each host has at least one benchmark
  if (
    error ||
    !index ||
    !Object.keys(index.hosts).length ||
    Object.values(index.hosts).some((h) => !h.benchmarks.length)
  ) {
    return (
      <div className="flex h-screen items-center justify-center text-destructive">
        {error ? `Error: ${error.message}` : "No hosts or benchmarks found"}
      </div>
    )
  }

  return (
    <UrlHostProvider>
      <div className="min-h-screen bg-background flex flex-col">
        <Header />
        <main className="mx-auto w-full max-w-7xl px-4 py-6 flex-1">
          <ErrorBoundary key={`${location.pathname}${location.search}`}>
            <Suspense fallback={<div className="text-muted-foreground">Loading...</div>}>
              <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/timeline" element={<Timeline />} />
                <Route path="/impact" element={<Impact />} />
                <Route path="/benchmark" element={<BenchmarkDetail />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </Suspense>
          </ErrorBoundary>
        </main>
        <Footer />
      </div>
    </UrlHostProvider>
  )
}

export default function App() {
  return (
    <ThemeProvider>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <AppContent />
        </BrowserRouter>

        {ReactQueryDevtools && (
          <Suspense fallback={null}>
            <ReactQueryDevtools initialIsOpen={false} />
          </Suspense>
        )}
      </QueryClientProvider>
    </ThemeProvider>
  )
}
