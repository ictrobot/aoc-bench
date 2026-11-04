import { Badge } from "@/components/ui/badge.tsx"

/**
 * Flags a measurement whose run values fell into more than one cluster. The exporter sets
 * this as an annotation only, so the mean and interval shown next to it are unchanged.
 */
export function MultimodalBadge({ className }: { className?: string }) {
  return (
    <Badge
      variant="outline"
      className={className ?? "font-sans text-xs text-muted-foreground"}
      title="The run values for this measurement fell into more than one cluster, so the mean sits between modes"
    >
      multimodal
    </Badge>
  )
}
