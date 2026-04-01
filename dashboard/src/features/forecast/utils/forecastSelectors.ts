import type {
  ConsumoForecastDiarioRow,
  ConsumoVs2024DiarioRow,
  ForecastChartModel,
  KpiCardModel,
  KpiCode,
  KpiGroupDefinition,
} from '@/types/forecast'
import { toQuarter, toYear, sortDates } from '@/utils/date'
import { formatDateLabel } from '@/utils/formatters'

type DailyValuePoint = {
  actual: number | null
  forecast: number | null
  value2024: number | null
}

type UnifiedKpiPoint = {
  actual: number | null
  forecast: number | null
  value2024: number | null
}

const safeAdd = (accumulator: number, value: number | null): number =>
  value === null ? accumulator : accumulator + value

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

const getAnalysisYear = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
): number => {
  const years = [...forecastRows, ...vsRows]
    .map((row) => toYear(row.fecha))
    .filter((year): year is number => year !== null)
  return years.length > 0 ? Math.max(...years) : new Date().getFullYear()
}

export const getDefaultSelectedQuarters = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
): number[] => {
  const analysisYear = getAnalysisYear(forecastRows, vsRows)
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

  return quarters.size > 0
    ? [...quarters].sort((a, b) => a - b)
    : [...ALL_QUARTERS]
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

const buildForecastLookup = (
  forecastRows: ConsumoForecastDiarioRow[],
  analysisYear: number,
  selectedQuarters: number[],
): Map<string, { actual: number | null; forecast: number | null }> => {
  const lookup = new Map<string, { actual: number | null; forecast: number | null }>()

  for (const row of forecastRows) {
    if (!shouldIncludeDate(row.fecha, analysisYear, selectedQuarters)) {
      continue
    }

    const key = `${row.fecha}__${row.kpi}`
    const current = lookup.get(key) ?? { actual: null, forecast: null }
    current.actual = mergeNullable(current.actual, row.actual_value)
    current.forecast = mergeNullable(current.forecast, row.forecast_value)
    lookup.set(key, current)
  }

  return lookup
}

const buildVsLookup = (
  vsRows: ConsumoVs2024DiarioRow[],
  analysisYear: number,
  selectedQuarters: number[],
): Map<string, { actualCurrent: number | null; value2024: number | null }> => {
  const lookup = new Map<
    string,
    { actualCurrent: number | null; value2024: number | null }
  >()

  for (const row of vsRows) {
    if (!shouldIncludeDate(row.fecha, analysisYear, selectedQuarters)) {
      continue
    }

    const key = `${row.fecha}__${row.kpi}`
    const current = lookup.get(key) ?? { actualCurrent: null, value2024: null }
    current.actualCurrent = mergeNullable(current.actualCurrent, row.actual_value_current)
    current.value2024 = mergeNullable(current.value2024, row.actual_value_2024)
    lookup.set(key, current)
  }

  return lookup
}

const buildDailyValuesByGroup = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
  selectedQuarters: number[],
): Map<string, Record<string, DailyValuePoint>> => {
  const analysisYear = getAnalysisYear(forecastRows, vsRows)
  const forecastLookup = buildForecastLookup(
    forecastRows,
    analysisYear,
    selectedQuarters,
  )
  const vsLookup = buildVsLookup(vsRows, analysisYear, selectedQuarters)
  const allKeys = new Set<string>([...forecastLookup.keys(), ...vsLookup.keys()])
  const rowsByDate = new Map<string, Record<string, DailyValuePoint>>()

  for (const key of allKeys) {
    const [date, kpi] = key.split('__')
    const groupId = KPI_TO_GROUP.get(kpi as KpiCode)
    if (!groupId) {
      continue
    }

    const forecastPoint = forecastLookup.get(key)
    const vsPoint = vsLookup.get(key)
    const unified: UnifiedKpiPoint = {
      actual: forecastPoint?.actual ?? vsPoint?.actualCurrent ?? null,
      forecast: forecastPoint?.forecast ?? null,
      value2024: vsPoint?.value2024 ?? null,
    }

    const byGroup = rowsByDate.get(date) ?? {}
    const existingPoint = byGroup[groupId] ?? {
      actual: null,
      forecast: null,
      value2024: null,
    }
    byGroup[groupId] = {
      actual:
        existingPoint.actual === null && unified.actual === null
          ? null
          : safeAdd(existingPoint.actual ?? 0, unified.actual),
      forecast:
        existingPoint.forecast === null && unified.forecast === null
          ? null
          : safeAdd(existingPoint.forecast ?? 0, unified.forecast),
      value2024:
        existingPoint.value2024 === null && unified.value2024 === null
          ? null
          : safeAdd(existingPoint.value2024 ?? 0, unified.value2024),
    }

    rowsByDate.set(date, byGroup)
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

export const buildChartModels = (
  forecastRows: ConsumoForecastDiarioRow[],
  vsRows: ConsumoVs2024DiarioRow[],
  selectedQuarters: number[],
): ForecastChartModel[] => {
  const rowsByDate = buildDailyValuesByGroup(forecastRows, vsRows, selectedQuarters)
  const sortedDates = sortDates([...rowsByDate.keys()])

  return getPrimaryChartGroups().map((group) => {
    const groupDates = sortedDates.filter((date) => {
      const point = rowsByDate.get(date)?.[group.id]
      return point
        ? point.actual !== null || point.forecast !== null || point.value2024 !== null
        : false
    })

    return {
      id: group.id,
      title: group.label,
      points: groupDates.map((date) => {
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
    }
  })
}
