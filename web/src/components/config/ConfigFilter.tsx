import { useId } from "react"
import { Combobox } from "@/components/ui/combobox.tsx"
import { shortenValue } from "@/lib/format.ts"

interface ConfigFilterProps {
  label: string
  values: string[]
  value: string
  onChange: (value: string) => void
  showAll?: boolean
  allLabel?: string
}

export function ConfigFilter({ label, values, value, onChange, showAll = true, allLabel = "All" }: ConfigFilterProps) {
  const controlId = useId()
  const labelId = `${controlId}-label`
  const options = [
    ...(showAll ? [{ value: "", label: allLabel }] : []),
    ...values.map((v) => ({ value: v, label: shortenValue(v) })),
  ]

  return (
    <div className="flex items-center gap-2">
      <label id={labelId} htmlFor={controlId} className="text-sm text-muted-foreground whitespace-nowrap">
        {label}:
      </label>
      <Combobox
        id={controlId}
        value={value}
        onChange={onChange}
        options={options}
        placeholder={showAll ? allLabel : "Select..."}
        searchPlaceholder={`Search ${label}...`}
        ariaLabelledBy={labelId}
        className="w-[160px]"
      />
    </div>
  )
}
