export const formatCompactNumber = (value: number | null): string => {
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
  const date = new Date(dateIso)
  if (Number.isNaN(date.getTime())) {
    return dateIso
  }

  return new Intl.DateTimeFormat('es-ES', {
    day: '2-digit',
    month: 'short',
  }).format(date)
}
