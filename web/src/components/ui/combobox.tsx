import { useState } from "react"
import { Check, ChevronsUpDown } from "lucide-react"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover.tsx"
import { Command, CommandEmpty, CommandInput, CommandItem, CommandList } from "@/components/ui/command.tsx"

interface ComboboxProps {
  id?: string
  value: string
  onChange: (value: string) => void
  options: { value: string; label: string }[]
  placeholder?: string
  searchPlaceholder?: string
  ariaLabel?: string
  ariaLabelledBy?: string
  className?: string
}

export function Combobox({
  id,
  value,
  onChange,
  options,
  placeholder = "Select...",
  searchPlaceholder = "Search...",
  ariaLabel,
  ariaLabelledBy,
  className,
}: ComboboxProps) {
  const [open, setOpen] = useState(false)
  const selected = options.find((o) => o.value === value)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          id={id}
          type="button"
          role="combobox"
          aria-haspopup="listbox"
          aria-expanded={open}
          aria-label={ariaLabelledBy ? undefined : (ariaLabel ?? placeholder)}
          aria-labelledby={ariaLabelledBy}
          className={`flex h-9 items-center justify-between rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-xs ring-offset-background focus:outline-none focus:ring-1 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50 ${className ?? "w-[200px]"}`}
        >
          <span className={selected ? "" : "text-muted-foreground"}>{selected ? selected.label : placeholder}</span>
          <ChevronsUpDown size={14} className="text-muted-foreground shrink-0 ml-2" />
        </button>
      </PopoverTrigger>
      <PopoverContent className="p-0" style={{ width: "var(--radix-popover-trigger-width)" }}>
        <Command filter={(value, search) => (value.toLowerCase().includes(search.toLowerCase()) ? 1 : 0)}>
          <CommandInput placeholder={searchPlaceholder} />
          <CommandList>
            <CommandEmpty>No results.</CommandEmpty>
            {options.map((o) => (
              <CommandItem
                key={o.value}
                value={o.label}
                onSelect={() => {
                  onChange(o.value)
                  setOpen(false)
                }}
              >
                <Check size={14} className={`mr-2 shrink-0 ${o.value === value ? "opacity-100" : "opacity-0"}`} />
                {o.label}
              </CommandItem>
            ))}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
