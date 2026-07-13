import { shortenValue } from "@/lib/format.ts"
import type { TimelineResultGroup } from "@/lib/timeline-grouping.ts"

interface GroupValueLabelProps {
  group: TimelineResultGroup
  /** When set, each value becomes a button that reports the clicked value. */
  onSelectValue?: (value: string) => void
}

/** A group's value(s): values in monospace, the range arrow in the UI font. */
export function GroupValueLabel({ group, onSelectValue }: GroupValueLabelProps) {
  const value = (v: string) =>
    onSelectValue ? (
      <button
        type="button"
        onClick={() => onSelectValue(v)}
        className="font-mono hover:underline"
        title={v}
        aria-label={`Open history for ${v}`}
      >
        {shortenValue(v)}
      </button>
    ) : (
      <span className="font-mono">{shortenValue(v)}</span>
    )

  if (group.caseCount === 1) {
    return value(group.startValue)
  }
  return (
    <>
      {value(group.startValue)}
      <span className="mx-1 font-sans text-muted-foreground">→</span>
      {value(group.endValue)}
    </>
  )
}
