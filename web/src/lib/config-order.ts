type ConfigKeys = Record<string, { values: string[] }>

export function compareConfigValueByOrder(
  a: string | undefined,
  b: string | undefined,
  orderedValues: string[],
): number {
  if (a === undefined && b === undefined) return 0
  if (a === undefined) return 1
  if (b === undefined) return -1

  const aIndex = orderedValues.indexOf(a)
  const bIndex = orderedValues.indexOf(b)
  if (aIndex !== -1 && bIndex !== -1) return aIndex - bIndex
  if (aIndex !== -1) return -1
  if (bIndex !== -1) return 1
  return a.localeCompare(b)
}

export function compareConfigsByOrder(
  a: Record<string, string>,
  b: Record<string, string>,
  configKeys: ConfigKeys,
): number {
  const orderedKeys = Object.keys(configKeys).sort()
  for (const key of orderedKeys) {
    const cmp = compareConfigValueByOrder(a[key], b[key], configKeys[key]?.values ?? [])
    if (cmp !== 0) return cmp
  }
  return 0
}
