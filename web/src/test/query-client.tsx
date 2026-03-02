import { QueryClient, QueryClientProvider, type QueryClientConfig } from "@tanstack/react-query"
import { render, type RenderOptions } from "@testing-library/react"
import type { ReactElement, ReactNode } from "react"
import { MemoryRouter } from "react-router"
import { UrlHostProvider } from "@/hooks/url-state-provider.tsx"

export function createTestQueryClient(config?: QueryClientConfig): QueryClient {
  const overrideDefaults = config?.defaultOptions
  return new QueryClient({
    ...config,
    defaultOptions: {
      ...overrideDefaults,
      queries: {
        retry: false,
        ...overrideDefaults?.queries,
      },
    },
  })
}

interface QueryRenderOptions extends Omit<RenderOptions, "wrapper"> {
  queryClient?: QueryClient
}

export function renderWithQueryClient(ui: ReactElement, options: QueryRenderOptions = {}) {
  const { queryClient = createTestQueryClient(), ...renderOptions } = options
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  }
  return {
    queryClient,
    ...render(ui, { wrapper: Wrapper, ...renderOptions }),
  }
}

interface RouterQueryRenderOptions extends QueryRenderOptions {
  initialEntries?: string[]
}

export function renderWithRouterAndQueryClient(ui: ReactElement, options: RouterQueryRenderOptions = {}) {
  const { initialEntries = ["/"], ...rest } = options
  return renderWithQueryClient(
    <MemoryRouter initialEntries={initialEntries}>
      <UrlHostProvider>{ui}</UrlHostProvider>
    </MemoryRouter>,
    rest,
  )
}
