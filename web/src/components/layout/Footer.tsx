import { useUrlHostBenchmark } from "@/hooks/use-url-state.tsx"
import { formatTimestampUtc } from "@/lib/format.ts"

export function Footer() {
  const { hostIndex } = useUrlHostBenchmark()

  return (
    <footer className="border-t mt-8">
      <div className="mx-auto flex h-10 max-w-7xl items-center justify-between px-4">
        <a
          href="https://github.com/ictrobot/aoc-bench"
          target="_blank"
          rel="noopener noreferrer"
          className="font-mono text-xs text-muted-foreground hover:text-foreground"
        >
          github.com/ictrobot/aoc-bench
        </a>
        <span className="text-xs text-muted-foreground">Last updated {formatTimestampUtc(hostIndex.last_updated)}</span>
      </div>
    </footer>
  )
}
