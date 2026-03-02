import { useEffect, useMemo } from "react"
import { Link } from "react-router"
import { useUrlHostBenchmark } from "@/hooks/use-url-state.tsx"
import { useBenchmarkResults } from "@/hooks/queries.ts"
import { formatDurationNs, formatCi, shortenValue } from "@/lib/format.ts"
import { Card, CardContent } from "@/components/ui/card.tsx"
import { withQuery } from "@/lib/routes.ts"
import { buildConfigSignature } from "@/lib/config-signature.ts"
import type { CompactResult, HostIndex } from "@/lib/types.ts"

const MAX_SPARK_POINTS = 100

export function BenchmarkDetail() {
  const { host, bench, hostIndex } = useUrlHostBenchmark()

  const { data: results, isLoading, error } = useBenchmarkResults(host, bench)

  useEffect(() => {
    document.title = `${bench} — aoc-bench`
  }, [bench])

  if (isLoading) return <div className="text-muted-foreground">Loading...</div>
  if (error) return <div className="text-destructive">Error: {error.message}</div>
  if (!results || results.length === 0) {
    return <div className="text-muted-foreground">No results found for this benchmark.</div>
  }

  return <BenchmarkDetailContent host={host} bench={bench} results={results} index={hostIndex} />
}

function BenchmarkDetailContent({
  host,
  bench,
  results,
  index,
}: {
  host: string
  bench: string
  results: CompactResult[]
  index: HostIndex
}) {
  const cards = useMemo(() => {
    // Find trendKey
    const keyCounts = new Map<string, Set<string>>()
    for (const r of results) {
      for (const [k, v] of Object.entries(r.config)) {
        if (!keyCounts.has(k)) keyCounts.set(k, new Set())
        keyCounts.get(k)!.add(v)
      }
    }
    const trendKey =
      index.timeline_key && keyCounts.has(index.timeline_key)
        ? index.timeline_key
        : ([...keyCounts.entries()].sort(([, a], [, b]) => b.size - a.size)[0]?.[0] ?? "")

    const canonicalTrendOrder = index.config_keys[trendKey]?.values ?? []

    // Group by config-minus-trendKey, keeping the result with the highest canonical trendKey
    // index as the headline number, and collecting all trendKey values for the sparkline.
    const groups = new Map<
      string,
      {
        config: Record<string, string>
        mean_ns: number
        ci95_half_ns: number
        representativeTrendIdx: number
        trendPoints: { trendValue: string; mean_ns: number }[]
      }
    >()

    for (const r of results) {
      const key = buildConfigSignature(r.config, trendKey)
      const trendValue = r.config[trendKey] ?? ""
      const trendIdx = canonicalTrendOrder.indexOf(trendValue)
      const existing = groups.get(key)
      if (!existing) {
        const groupConfig = Object.fromEntries(Object.entries(r.config).filter(([k]) => k !== trendKey))
        groups.set(key, {
          config: groupConfig,
          mean_ns: r.mean_ns,
          ci95_half_ns: r.ci95_half_ns,
          representativeTrendIdx: trendIdx,
          trendPoints: [{ trendValue, mean_ns: r.mean_ns }],
        })
      } else {
        existing.trendPoints.push({ trendValue, mean_ns: r.mean_ns })
        if (trendIdx > existing.representativeTrendIdx) {
          existing.mean_ns = r.mean_ns
          existing.ci95_half_ns = r.ci95_half_ns
          existing.representativeTrendIdx = trendIdx
        }
      }
    }

    // Build spark data per group
    return [...groups.entries()]
      .sort(([, a], [, b]) => {
        for (const key of Object.keys(a.config)) {
          const order = index.config_keys[key]?.values ?? []
          const ai = order.indexOf(a.config[key])
          const bi = order.indexOf(b.config[key])
          if (ai !== bi) return (ai === -1 ? Infinity : ai) - (bi === -1 ? Infinity : bi)
        }
        return 0
      })
      .map(([key, group]) => {
        // Order trendPoints by canonical order
        const pointMap = new Map(group.trendPoints.map((p) => [p.trendValue, p.mean_ns]))
        const ordered = [
          ...canonicalTrendOrder.filter((v) => pointMap.has(v)),
          ...group.trendPoints.map((p) => p.trendValue).filter((v) => !canonicalTrendOrder.includes(v)),
        ]

        // Downsample to MAX_SPARK_POINTS
        const step = ordered.length > MAX_SPARK_POINTS ? ordered.length / MAX_SPARK_POINTS : 1
        const sampled =
          step <= 1
            ? ordered
            : Array.from({ length: MAX_SPARK_POINTS }, (_, i) => {
                if (i === 0) return ordered[0]
                if (i === MAX_SPARK_POINTS - 1) return ordered[ordered.length - 1]
                return ordered[Math.min(Math.round(i * step), ordered.length - 2)]
              })

        const sparkData = sampled.map((v) => ({
          value: pointMap.get(v)!,
        }))

        const filterParams = Object.fromEntries(Object.entries(group.config).map(([k, v]) => [`f_${k}`, v]))

        return {
          key,
          config: group.config,
          mean_ns: group.mean_ns,
          ci95_half_ns: group.ci95_half_ns,
          sparkData,
          timelineUrl: withQuery("/timeline", { host, bench, ...filterParams }),
        }
      })
  }, [results, index, host, bench])

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">{bench}</h1>
      <CardGrid cards={cards} />
    </div>
  )
}

interface CardData {
  key: string
  config: Record<string, string>
  mean_ns: number
  ci95_half_ns: number
  sparkData: { value: number }[]
  timelineUrl: string
}

function CardGrid({ cards }: { cards: CardData[] }) {
  return (
    <div className="grid grid-cols-1 gap-4 pb-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
      {cards.map((card) => (
        <ConfigCard
          key={card.key}
          config={card.config}
          mean_ns={card.mean_ns}
          ci95_half_ns={card.ci95_half_ns}
          sparkData={card.sparkData}
          timelineUrl={card.timelineUrl}
        />
      ))}
    </div>
  )
}

function ConfigCard({ config, mean_ns, ci95_half_ns, sparkData, timelineUrl }: CardData) {
  return (
    <Link to={timelineUrl}>
      <Card className="hover:bg-muted/50 transition-colors cursor-pointer h-full">
        <CardContent className="p-5 flex flex-col gap-3">
          {Object.entries(config).length > 0 && (
            <div className="flex flex-wrap gap-x-3 gap-y-1">
              {Object.entries(config).map(([k, v]) => (
                <div key={k} className="text-sm text-muted-foreground">
                  <span className="font-medium text-foreground">{k}</span> {shortenValue(v)}
                </div>
              ))}
            </div>
          )}
          <div>
            <div className="text-3xl font-bold tabular-nums">{formatDurationNs(mean_ns)}</div>
            <div className="text-sm text-muted-foreground">{formatCi(ci95_half_ns)}</div>
          </div>
          {sparkData.length > 1 && <SparkBar data={sparkData} />}
        </CardContent>
      </Card>
    </Link>
  )
}

function SparkBar({ data }: { data: { value: number }[] }) {
  const max = Math.max(...data.map((d) => d.value)) || 1
  const W = data.length
  const H = 80
  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} preserveAspectRatio="none">
      {data.map((d, i) => {
        const h = Math.max(0.5, (d.value / max) * H)
        return <rect key={i} x={i} y={H - h} width={1} height={h} fill="var(--color-muted-foreground)" />
      })}
    </svg>
  )
}
