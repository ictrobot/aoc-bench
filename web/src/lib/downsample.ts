const MAX_SPARK_BUCKETS = 256

/**
 * Bound sparkline size for very long histories: bucket the series and keep
 * each bucket's min and max (in encounter order), so steps and one-off spikes
 * survive downsampling — unlike every-kth-point sampling, which can skip them.
 */
export function downsampleSparkValues(values: number[]): number[] {
  if (values.length <= MAX_SPARK_BUCKETS * 2) return values
  const out: number[] = []
  const bucketSize = values.length / MAX_SPARK_BUCKETS
  for (let b = 0; b < MAX_SPARK_BUCKETS; b++) {
    const start = Math.floor(b * bucketSize)
    const end = Math.min(values.length, Math.max(start + 1, Math.floor((b + 1) * bucketSize)))
    let minIndex = start
    let maxIndex = start
    for (let i = start + 1; i < end; i++) {
      if (values[i] < values[minIndex]) minIndex = i
      if (values[i] > values[maxIndex]) maxIndex = i
    }
    if (minIndex === maxIndex) {
      out.push(values[minIndex])
    } else if (minIndex < maxIndex) {
      out.push(values[minIndex], values[maxIndex])
    } else {
      out.push(values[maxIndex], values[minIndex])
    }
  }
  return out
}
