export const toYear = (dateIso: string): number | null => {
  const parsed = new Date(dateIso)
  if (Number.isNaN(parsed.getTime())) {
    return null
  }
  return parsed.getFullYear()
}

export const toQuarter = (dateIso: string): number | null => {
  const parsed = new Date(dateIso)
  if (Number.isNaN(parsed.getTime())) {
    return null
  }
  return Math.floor(parsed.getMonth() / 3) + 1
}

export const sortDates = (dates: string[]): string[] =>
  [...dates].sort((a, b) => new Date(a).getTime() - new Date(b).getTime())
