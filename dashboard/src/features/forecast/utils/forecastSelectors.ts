import type {
  ConsumoForecastDiarioRow,
  ConsumoVs2024DiarioRow,
  ForecastChartModel,
  KpiCardModel,
  KpiGroupDefinition,
} from '@/types/forecast'
import { toQuarter, toYear, sortDates } from '@/utils/date'
import { formatDateLabel } from '@/utils/formatters'

type DailyValuePoint = {
  actual: number | null
  forecast: number | null
  value2024: number | null
}

const safeAdd = (accumulator: number, value: number | null): number =>
  value === null ? accumulator : accumulator + value

const safeRatio = (
  actualValue: number | null,
  referenceValue: number | null,
): number | null => {
  if (
    actualValue === null ||
    referenceValue === null ||
    Number.isNaN(referenceValue) ||
    referenceValue === 0
  ) {
    return null
  }

  return (actualValue - referenceValue) / Math.abs(referenceValue)
}

export const KPI_GROUPS: KpiGroupDefinition[] = [
  {
    id: 'entregas',
    label: 'Entregas',
    kpis: ['entregas_sge', 'entregas_sgp'],
    accentColor: 'var(--kpi-blue)',
  },
  {
    id: 'recogidas',
    label: 'Recogidas',
    kpis: ['recogidas_ege'],
    accentColor: 'var(--kpi-amber)',
  },
  {
    id: 'lineas_preparadas',
    label: 'Líneas preparadas',
    kpis: ['picking_lines_pi'],
    accentColor: 'var(--kpi-indigo)',
  },
  {
    id: 'unidades_preparadas',
    label: 'Unidades preparadas',
    kpis: ['picking_units_pi'],
    accentColor: 'var(--kpi-teal)',
  },
]

const PRIMARY_CHART_GROUP_IDS: Array<KpiGroupDefinition['id']> = [
  'entregas',
  'recogidas',
  'lineas_preparadas',
]

export const getPrimaryChartGroups = (): KpiGroupDefinition[] =>
  KPI_GROUPS.filter((group) => PRIMARY_CHART_GROUP_IDS.includes(group.id))

export const getAvailableQuarters = (
  forecastRows: ConsumoForecastDiarioRow[],
): number[] => {
  const years = forecastRows
    .map((row) => toYear(row.fecha))
    .filter((year): year is number => year !== null)
  if (years.length === 0) {
    return [1, 2, 3, 4]
  }

  const currentYear = Math.max(...years)
  const quarters = new Set<number>()
  for (const row of forecastRows) {
    if (toYear(row.fecha) !== currentYear) {
      continue
    }
    const quarter = toQuarter(row.fecha)
    if (quarter !== null) {
      quarters.add(quarter)
    }
  }

  return quarters.size > 0 ? [...quarters].sort((a, b) => a - b) : [1, 2, 3, 4]
}

const getCurrentYear = (forecastRows: ConsumoForecastDiarioRow[]): number => {
  const years = forecastRows
    .map((row) => toYear(row.fecha))
    .filter((year): year is number => year !== null)
  return years.length > 0 ? Math.max(...years) : new Date().getFullYear()
}

const shouldIncludeDate = (
  dateIso: string,
  currentYear: number,
  selectedQuarters: number[],
): boolean => {
  if (toYear(dateIso) !== currentYear) {
    return false
  }

  if (selectedQuarters.length === 0) {
    return false
  }

  const quarter = toQuarter(dateIso)
  return quarter !== null && selectedQuarters.includes(quarter)
}

const buildVs2024Lookup = (
  vsRows: ConsumoVs2024DiarioRow[],
): Map<string, number | null> => {
  const lookup = new Map<string, number | null>()
  for (const row of vsRows) {
    lookup.set(`${row.fecha}__${row.kpi}`, row.actual_value_2024)
  }
  return lookup
}

const buildDailyValuesByGroup = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
  selectedQuarters: number[],
): Map<string, Record<string, DailyValuePoint>> => {
  const currentYear = getCurrentYear(forecastRows)
  const vs2024Lookup = buildVs2024Lookup(vsRows)
  const rowsByDate = new Map<string, Record<string, DailyValuePoint>>()

  for (const row of forecastRows) {
    if (!shouldIncludeDate(row.fecha, currentYear, selectedQuarters)) {
      continue
    }

    const group = KPI_GROUPS.find((item) => item.kpis.includes(row.kpi))
    if (!group) {
      continue
    }

    const byGroup = rowsByDate.get(row.fecha) ?? {}
    const existingPoint = byGroup[group.id] ?? {
      actual: null,
      forecast: null,
      value2024: null,
    }

    const actual2024 = vs2024Lookup.get(`${row.fecha}__${row.kpi}`) ?? null
    byGroup[group.id] = {
      actual:
        existingPoint.actual === null && row.actual_value === null
          ? null
          : safeAdd(existingPoint.actual ?? 0, row.actual_value),
      forecast:
        existingPoint.forecast === null && row.forecast_value === null
          ? null
          : safeAdd(existingPoint.forecast ?? 0, row.forecast_value),
      value2024:
        existingPoint.value2024 === null && actual2024 === null
          ? null
          : safeAdd(existingPoint.value2024 ?? 0, actual2024),
    }

    rowsByDate.set(row.fecha, byGroup)
  }

  return rowsByDate
}

export const buildKpiCards = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
  selectedQuarters: number[],
): KpiCardModel[] => {
  const rowsByDate = buildDailyValuesByGroup(forecastRows, vsRows, selectedQuarters)

  return KPI_GROUPS.map((group) => {
    let actualTotal = 0
    let forecastTotal = 0
    let total2024 = 0
    let hasActual = false
    let hasForecast = false
    let has2024 = false

    for (const groupedData of rowsByDate.values()) {
      const point = groupedData[group.id]
      if (!point) {
        continue
      }
      if (point.actual !== null) {
        actualTotal += point.actual
        hasActual = true
      }
      if (point.forecast !== null) {
        forecastTotal += point.forecast
        hasForecast = true
      }
      if (point.value2024 !== null) {
        total2024 += point.value2024
        has2024 = true
      }
    }

    const actualValue = hasActual ? actualTotal : null
    const forecastValue = hasForecast ? forecastTotal : null
    const value2024 = has2024 ? total2024 : null
    return {
      id: group.id,
      title: group.label,
      valueActual: actualValue,
      valueForecast: forecastValue,
      value2024,
      deltaVsForecastPct: safeRatio(actualValue, forecastValue),
      deltaVs2024Pct: safeRatio(actualValue, value2024),
      hasData: actualValue !== null || forecastValue !== null || value2024 !== null,
    }
  })
}

export const buildChartModels = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
  selectedQuarters: number[],
): ForecastChartModel[] => {
  const rowsByDate = buildDailyValuesByGroup(forecastRows, vsRows, selectedQuarters)
  const sortedDates = sortDates([...rowsByDate.keys()])

  return getPrimaryChartGroups().map((group) => ({
    id: group.id,
    title: group.label,
    points: sortedDates.map((date) => {
      const point = rowsByDate.get(date)?.[group.id] ?? {
        actual: null,
        forecast: null,
        value2024: null,
      }
      return {
        fecha: date,
        label: formatDateLabel(date),
        actual: point.actual,
        forecast: point.forecast,
        value2024: point.value2024,
      }
    }),
  }))
}
