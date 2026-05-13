import { parseIsoDate } from '@/utils/date'

export const formatCompactNumber = (value: number | null): string => {
  if (value === null || Number.isNaN(value)) {
    return '--'
  }

  return new Intl.NumberFormat('es-ES', {
    maximumFractionDigits: 0,
  }).format(value)
}

export const formatNumber = (value: number | null): string => {
  if (value === null || Number.isNaN(value)) {
    return '--'
  }

  return new Intl.NumberFormat('es-ES', {
    maximumFractionDigits: 0,
  }).format(value)
}

export const formatPercent = (value: number | null): string => {
  if (value === null || Number.isNaN(value)) {
    return '--'
  }

  return `${value >= 0 ? '+' : ''}${(value * 100).toFixed(1)}%`
}

export const formatDateLabel = (dateIso: string): string => {
  const date = parseIsoDate(dateIso)
  if (date === null) {
    return dateIso
  }

  return new Intl.DateTimeFormat('es-ES', {
    day: '2-digit',
    month: 'short',
  }).format(date)
}

const SPANISH_MONTH_ABBREVIATIONS = [
  'ENE',
  'FEB',
  'MAR',
  'ABR',
  'MAY',
  'JUN',
  'JUL',
  'AGO',
  'SEP',
  'OCT',
  'NOV',
  'DIC',
] as const

const getIsoWeekNumber = (date: Date): number => {
  const copy = new Date(date)
  copy.setHours(0, 0, 0, 0)
  copy.setDate(copy.getDate() + 3 - ((copy.getDay() + 6) % 7))
  const firstThursday = new Date(copy.getFullYear(), 0, 4)
  firstThursday.setHours(0, 0, 0, 0)
  firstThursday.setDate(
    firstThursday.getDate() + 3 - ((firstThursday.getDay() + 6) % 7),
  )
  const diffInDays = (copy.getTime() - firstThursday.getTime()) / 86400000
  return 1 + Math.round(diffInDays / 7)
}

export const formatWeekLabel = (weekStartDateIso: string): string => {
  const date = parseIsoDate(weekStartDateIso)
  if (date === null) {
    return weekStartDateIso
  }

  const weekNumber = getIsoWeekNumber(date)
  const month = SPANISH_MONTH_ABBREVIATIONS[date.getMonth()]
  return `WK${String(weekNumber).padStart(2, '0')} - ${date.getDate()}${month}`
}
