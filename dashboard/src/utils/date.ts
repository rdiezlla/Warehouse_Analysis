const ISO_DATE_PATTERN = /^(\d{4})-(\d{2})-(\d{2})$/

export const parseIsoDate = (dateIso: string): Date | null => {
  const match = ISO_DATE_PATTERN.exec(dateIso)
  if (match) {
    const [, year, month, day] = match
    return new Date(Number(year), Number(month) - 1, Number(day))
  }

  const parsed = new Date(dateIso)
  if (Number.isNaN(parsed.getTime())) {
    return null
  }

  return parsed
}

export const toYear = (dateIso: string): number | null => {
  const parsed = parseIsoDate(dateIso)
  if (parsed === null) {
    return null
  }
  return parsed.getFullYear()
}

export const toQuarter = (dateIso: string): number | null => {
  const parsed = parseIsoDate(dateIso)
  if (parsed === null) {
    return null
  }
  return Math.floor(parsed.getMonth() / 3) + 1
}

export const sortDates = (dates: string[]): string[] =>
  [...dates].sort((a, b) => {
    const first = parseIsoDate(a)
    const second = parseIsoDate(b)
    return (first?.getTime() ?? 0) - (second?.getTime() ?? 0)
  })
