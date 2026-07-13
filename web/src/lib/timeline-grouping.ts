import { buildConfigSignature } from "./config-signature.ts"
import type { CompactResult } from "./types.ts"

export interface TimelineResultGroup {
  fullValue: string
  startValue: string
  endValue: string
  startIndex: number
  endIndex: number
  mean_ns: number
  ci95_half_ns: number
  configs: Record<string, string>[]
  measurement_token: number
  caseCount: number
  annotations: { value: string; label: string }[]
  fixedSignature: string
}

/** Coalesce only contiguous cases backed by the exact same shared measurement. */
export function groupTimelineResults(
  sortedResults: CompactResult[],
  varyingKey: string,
  valueOrder: string[],
  annotations: Record<string, string> = {},
): TimelineResultGroup[] {
  const groups: TimelineResultGroup[] = []
  for (const result of sortedResults) {
    const value = result.config[varyingKey] ?? ""
    const valueIndex = valueOrder.indexOf(value)
    const annotation = annotations[value]
    const fixedSignature = buildConfigSignature(result.config, varyingKey)
    const previous = groups.at(-1)
    const canMerge =
      previous !== undefined &&
      result.measurement_token !== 0 &&
      result.measurement_token === previous.measurement_token &&
      result.mean_ns === previous.mean_ns &&
      result.ci95_half_ns === previous.ci95_half_ns &&
      fixedSignature === previous.fixedSignature &&
      previous.endIndex >= 0 &&
      valueIndex === previous.endIndex + 1

    if (canMerge) {
      previous.endValue = value
      previous.endIndex = valueIndex
      previous.fullValue = `${previous.startValue}–${value}`
      previous.configs.push(result.config)
      if (annotation) previous.annotations.push({ value, label: annotation })
      previous.caseCount++
      continue
    }

    groups.push({
      fullValue: value,
      startValue: value,
      endValue: value,
      startIndex: valueIndex,
      endIndex: valueIndex,
      mean_ns: result.mean_ns,
      ci95_half_ns: result.ci95_half_ns,
      configs: [result.config],
      measurement_token: result.measurement_token,
      caseCount: 1,
      annotations: annotation ? [{ value, label: annotation }] : [],
      fixedSignature,
    })
  }
  return groups
}
