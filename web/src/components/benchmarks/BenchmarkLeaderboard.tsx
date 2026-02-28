import { useNavigate } from "react-router-dom"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card.tsx"
import { formatDurationNs } from "@/lib/format.ts"
import { withQuery } from "@/lib/routes.ts"

interface LeaderboardEntry {
  name: string
  mean_ns: number
}

interface Props {
  entries: LeaderboardEntry[]
  host: string
  n?: number
}

export function BenchmarkLeaderboard({ entries, host, n = 10 }: Props) {
  if (entries.length === 0) return null

  const sorted = [...entries].sort((a, b) => a.mean_ns - b.mean_ns)
  const fastest = sorted.slice(0, n)
  const slowest = sorted.slice(-n).reverse()

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <LeaderboardPanel title="Fastest" entries={fastest} host={host} />
      <LeaderboardPanel title="Slowest" entries={slowest} host={host} />
    </div>
  )
}

function LeaderboardPanel({ title, entries, host }: { title: string; entries: LeaderboardEntry[]; host: string }) {
  const navigate = useNavigate()

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <ol>
          {entries.map((entry, i) => (
            <li key={entry.name} className="border-b last:border-b-0">
              <button
                type="button"
                className="flex w-full items-center gap-3 px-4 py-2 text-left hover:bg-muted/50 transition-colors"
                onClick={() => navigate(withQuery("/benchmark", { host, bench: entry.name }))}
                aria-label={`Open benchmark ${entry.name}`}
              >
                <span className="text-sm text-muted-foreground w-5 text-right shrink-0">{i + 1}.</span>
                <span className="flex-1 text-sm truncate">{entry.name}</span>
                <span className="text-sm font-mono tabular-nums shrink-0">{formatDurationNs(entry.mean_ns)}</span>
              </button>
            </li>
          ))}
        </ol>
      </CardContent>
    </Card>
  )
}
