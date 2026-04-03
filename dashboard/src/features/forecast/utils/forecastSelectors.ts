import type {
  ConsumoForecastDiarioRow,
  ConsumoForecastSemanalRow,
  ConsumoVs2024DiarioRow,
  ConsumoVs2024SemanalRow,
  ForecastChartModel,
  KpiCardModel,
  KpiCode,
  KpiGroupDefinition,
} from '@/types/forecast'
import { sortDates, toQuarter, toYear } from '@/utils/date'
import { formatWeekLabel } from '@/utils/formatters'

type AggregatedValuePoint = {
  actual: number | null
  forecast: number | null
  value2024: number | null
}

const mergeNullable = (
  current: number | null,
  nextValue: number | null,
): number | null => {
  if (nextValue === null) {
    return current
  }
  if (current === null) {
    return nextValue
  }
  return current + nextValue
}

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

const ALL_QUARTERS = [1, 2, 3, 4] as const

const KPI_TO_GROUP = new Map<KpiCode, KpiGroupDefinition['id']>(
  KPI_GROUPS.flatMap((group) => group.kpis.map((kpi) => [kpi, group.id] as const)),
)

export const getPrimaryChartGroups = (): KpiGroupDefinition[] =>
  KPI_GROUPS.filter((group) => PRIMARY_CHART_GROUP_IDS.includes(group.id))

export const getAvailableQuarters = (): number[] => [...ALL_QUARTERS]

const getAnalysisYearFromDates = (dates: string[]): number => {
  const years = dates
    .map((date) => toYear(date))
    .filter((year): year is number => year !== null)

  return years.length > 0 ? Math.max(...years) : new Date().getFullYear()
}

const shouldIncludeDate = (
  dateIso: string,
  analysisYear: number,
  selectedQuarters: number[],
): boolean => {
  if (selectedQuarters.length === 0) {
    return false
  }

  if (toYear(dateIso) !== analysisYear) {
    return false
  }

  const quarter = toQuarter(dateIso)
  return quarter !== null && selectedQuarters.includes(quarter)
}

export const getDefaultSelectedQuarters = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
): number[] => {
  const analysisYear = getAnalysisYearFromDates([
    ...forecastRows.map((row) => row.fecha),
    ...vsRows.map((row) => row.fecha),
  ])
  const quarters = new Set<number>()

  for (const row of forecastRows) {
    if (toYear(row.fecha) !== analysisYear) {
      continue
    }
    const quarter = toQuarter(row.fecha)
    if (quarter !== null) {
      quarters.add(quarter)
    }
  }

  if (quarters.size === 0) {
    for (const row of vsRows) {
      if (toYear(row.fecha) !== analysisYear) {
        continue
      }
      const quarter = toQuarter(row.fecha)
      if (quarter !== null) {
        quarters.add(quarter)
      }
    }
  }

  return quarters.size > 0 ? [...quarters].sort((a, b) => a - b) : [...ALL_QUARTERS]
}

const buildDailyValuesByGroup = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
  selectedQuarters: number[],
): Map<string, Record<string, AggregatedValuePoint>> => {
  const analysisYear = getAnalysisYearFromDates([
    ...forecastRows.map((row) => row.fecha),
    ...vsRows.map((row) => row.fecha),
  ])
  const rowsByDate = new Map<string, Record<string, AggregatedValuePoint>>()

  for (const row of forecastRows) {
    if (!shouldIncludeDate(row.fecha, analysisYear, selectedQuarters)) {
      continue
    }

    const groupId = KPI_TO_GROUP.get(row.kpi)
    if (!groupId) {
      continue
    }

    const byGroup = rowsByDate.get(row.fecha) ?? {}
    const point = byGroup[groupId] ?? {
      actual: null,
      forecast: null,
      value2024: null,
    }
    byGroup[groupId] = {
      ...point,
      actual: mergeNullable(point.actual, row.actual_value),
      forecast: mergeNullable(point.forecast, row.forecast_value),
    }
    rowsByDate.set(row.fecha, byGroup)
  }

  for (const row of vsRows) {
    if (!shouldIncludeDate(row.fecha, analysisYear, selectedQuarters)) {
      continue
    }

    const groupId = KPI_TO_GROUP.get(row.kpi)
    if (!groupId) {
      continue
    }

    const byGroup = rowsByDate.get(row.fecha) ?? {}
    const point = byGroup[groupId] ?? {
      actual: null,
      forecast: null,
      value2024: null,
    }
    byGroup[groupId] = {
      actual: mergeNullable(point.actual, row.actual_value_current),
      forecast: point.forecast,
      value2024: mergeNullable(point.value2024, row.actual_value_2024),
    }
    rowsByDate.set(row.fecha, byGroup)
  }

  return rowsByDate
}

const buildWeeklyValuesByGroup = (
  forecastRows: ConsumoForecastSemanalRow[],
  vsRows: ConsumoVs2024SemanalRow[],
  selectedQuarters: number[],
): Map<string, Record<string, AggregatedValuePoint>> => {
  const analysisYear = getAnalysisYearFromDates([
    ...forecastRows.map((row) => row.week_start_date),
    ...vsRows.map((row) => row.week_start_date),
  ])
  const rowsByWeek = new Map<string, Record<string, AggregatedValuePoint>>()

  for (const row of forecastRows) {
    if (!shouldIncludeDate(row.week_start_date, analysisYear, selectedQuarters)) {
      continue
    }

    const groupId = KPI_TO_GROUP.get(row.kpi)
    if (!groupId) {
      continue
    }

    const byGroup = rowsByWeek.get(row.week_start_date) ?? {}
    const point = byGroup[groupId] ?? {
      actual: null,
      forecast: null,
      value2024: null,
    }
    byGroup[groupId] = {
      ...point,
      actual: mergeNullable(point.actual, row.actual_value),
      forecast: mergeNullable(point.forecast, row.forecast_value),
    }
    rowsByWeek.set(row.week_start_date, byGroup)
  }

  for (const row of vsRows) {
    if (!shouldIncludeDate(row.week_start_date, analysisYear, selectedQuarters)) {
      continue
    }

    const groupId = KPI_TO_GROUP.get(row.kpi)
    if (!groupId) {
      continue
    }

    const byGroup = rowsByWeek.get(row.week_start_date) ?? {}
    const point = byGroup[groupId] ?? {
      actual: null,
      forecast: null,
      value2024: null,
    }
    byGroup[groupId] = {
      actual: mergeNullable(point.actual, row.actual_value_current),
      forecast: point.forecast,
      value2024: mergeNullable(point.value2024, row.actual_value_2024),
    }
    rowsByWeek.set(row.week_start_date, byGroup)
  }

  return rowsByWeek
}

export const buildKpiCards = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
  selectedQuarters: number[],
): KpiCardModel[] => {
  const rowsByDate = buildDailyValuesByGroup(forecastRows, vsRows, selectedQuarters)

  return KPI_GROUPS.map((group) => {
    let actualTotal: number | null = null
    let forecastTotal: number | null = null
    let total2024: number | null = null

    for (const groupedData of rowsByDate.values()) {
      const point = groupedData[group.id]
      if (!point) {
        continue
      }

      if (point.actual !== null) {
        actualTotal = mergeNullable(actualTotal, point.actual)
      }
      if (point.forecast !== null) {
        forecastTotal = mergeNullable(forecastTotal, point.forecast)
      }
      if (point.value2024 !== null) {
        total2024 = mergeNullable(total2024, point.value2024)
      }
    }

    const actualValue = actualTotal
    const forecastValue = forecastTotal
    const value2024 = total2024
    const headlineValue = actualValue ?? forecastValue ?? value2024
    const headlineLabel =
      actualValue !== null
        ? 'Actual'
        : forecastValue !== null
          ? 'Forecast (sin actual)'
          : value2024 !== null
            ? '2024 (sin actual)'
            : 'Sin datos'

    return {
      id: group.id,
      title: group.label,
      valueActual: actualValue,
      valueForecast: forecastValue,
      value2024,
      headlineValue,
      headlineLabel,
      deltaVsForecastPct: safeRatio(actualValue, forecastValue),
      deltaVs2024Pct: safeRatio(actualValue, value2024),
      hasData: headlineValue !== null,
    }
  })
}

export const buildWeeklyChartModels = (
  forecastRows: ConsumoForecastSemanalRow[],
  vsRows: ConsumoVs2024SemanalRow[],
  selectedQuarters: number[],
): ForecastChartModel[] => {
  const rowsByWeek = buildWeeklyValuesByGroup(forecastRows, vsRows, selectedQuarters)
  const sortedWeeks = sortDates([...rowsByWeek.keys()])

  return getPrimaryChartGroups().map((group) => {
    const groupWeeks = sortedWeeks.filter((weekStartDate) => {
      const point = rowsByWeek.get(weekStartDate)?.[group.id]
      return (
        point?.actual !== null ||
        point?.forecast !== null ||
        point?.value2024 !== null
      )
    })

    return {
      id: group.id,
      title: group.label,
      points: groupWeeks.map((weekStartDate) => {
        const point = rowsByWeek.get(weekStartDate)?.[group.id] ?? {
          actual: null,
          forecast: null,
          value2024: null,
        }

        return {
          fecha: weekStartDate,
          label: formatWeekLabel(weekStartDate),
          actual: point.actual,
          forecast: point.forecast,
          value2024: point.value2024,
        }
      }),
    }
  })
}
