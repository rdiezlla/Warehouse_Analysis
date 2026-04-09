import type {
  ConsumoForecastDiarioRow,
  ConsumoForecastSemanalRow,
  ConsumoVs2024DiarioRow,
  ConsumoVs2024SemanalRow,
  ForecastChartModel,
  ForecastChartPoint,
  ForecastChartViewId,
  ForecastChartViewModel,
  KpiCardModel,
  KpiCode,
  KpiGroupDefinition,
  KpiGroupId,
  PickingChartViewId,
  ServicesChartViewId,
} from '@/types/forecast'
import { sortDates, toQuarter, toYear } from '@/utils/date'
import { formatWeekLabel } from '@/utils/formatters'

type AggregatedValuePoint = {
  actual: number | null
  forecast: number | null
  value2024: number | null
}

type GroupedValuesByKpi = Partial<Record<KpiGroupId, AggregatedValuePoint>>

type ChartViewDefinition<TViewId extends string> = {
  id: TViewId
  label: string
  groupIds: KpiGroupId[]
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

const createEmptyAggregatedValuePoint = (): AggregatedValuePoint => ({
  actual: null,
  forecast: null,
  value2024: null,
})

const hasAggregatedValue = (point: AggregatedValuePoint): boolean =>
  point.actual !== null || point.forecast !== null || point.value2024 !== null

const sumAggregatedValuePoints = (
  points: AggregatedValuePoint[],
): AggregatedValuePoint =>
  points.reduce<AggregatedValuePoint>(
    (accumulator, point) => ({
      actual: mergeNullable(accumulator.actual, point.actual),
      forecast: mergeNullable(accumulator.forecast, point.forecast),
      value2024: mergeNullable(accumulator.value2024, point.value2024),
    }),
    createEmptyAggregatedValuePoint(),
  )

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

const ALL_QUARTERS = [1, 2, 3, 4] as const

const KPI_TO_GROUP = new Map<KpiCode, KpiGroupDefinition['id']>(
  KPI_GROUPS.flatMap((group) => group.kpis.map((kpi) => [kpi, group.id] as const)),
)

const SERVICES_CHART_VIEWS: ChartViewDefinition<ServicesChartViewId>[] = [
  {
    id: 'entregas',
    label: 'Entregas',
    groupIds: ['entregas'],
  },
  {
    id: 'recogidas',
    label: 'Recogidas',
    groupIds: ['recogidas'],
  },
  {
    id: 'entregas_recogidas',
    label: 'Entregas + Recogidas',
    groupIds: ['entregas', 'recogidas'],
  },
]

const PICKING_CHART_VIEWS: ChartViewDefinition<PickingChartViewId>[] = [
  {
    id: 'lineas',
    label: 'Líneas',
    groupIds: ['lineas_preparadas'],
  },
  {
    id: 'unidades',
    label: 'Unidades',
    groupIds: ['unidades_preparadas'],
  },
]

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
): Map<string, GroupedValuesByKpi> => {
  const analysisYear = getAnalysisYearFromDates([
    ...forecastRows.map((row) => row.fecha),
    ...vsRows.map((row) => row.fecha),
  ])
  const rowsByDate = new Map<string, GroupedValuesByKpi>()

  for (const row of forecastRows) {
    if (!shouldIncludeDate(row.fecha, analysisYear, selectedQuarters)) {
      continue
    }

    const groupId = KPI_TO_GROUP.get(row.kpi)
    if (!groupId) {
      continue
    }

    const byGroup = rowsByDate.get(row.fecha) ?? {}
    const point = byGroup[groupId] ?? createEmptyAggregatedValuePoint()
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
    const point = byGroup[groupId] ?? createEmptyAggregatedValuePoint()
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
): Map<string, GroupedValuesByKpi> => {
  const analysisYear = getAnalysisYearFromDates([
    ...forecastRows.map((row) => row.week_start_date),
    ...vsRows.map((row) => row.week_start_date),
  ])
  const rowsByWeek = new Map<string, GroupedValuesByKpi>()

  for (const row of forecastRows) {
    if (!shouldIncludeDate(row.week_start_date, analysisYear, selectedQuarters)) {
      continue
    }

    const groupId = KPI_TO_GROUP.get(row.kpi)
    if (!groupId) {
      continue
    }

    const byGroup = rowsByWeek.get(row.week_start_date) ?? {}
    const point = byGroup[groupId] ?? createEmptyAggregatedValuePoint()
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
    const point = byGroup[groupId] ?? createEmptyAggregatedValuePoint()
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

  const buildPointsForGroups = (groupIds: KpiGroupId[]): ForecastChartPoint[] => {
    const groupWeeks = sortedWeeks.filter((weekStartDate) => {
      const aggregatedPoint = sumAggregatedValuePoints(
        groupIds.map(
          (groupId) =>
            rowsByWeek.get(weekStartDate)?.[groupId] ?? createEmptyAggregatedValuePoint(),
        ),
      )

      return hasAggregatedValue(aggregatedPoint)
    })

    return groupWeeks.map((weekStartDate) => {
      const aggregatedPoint = sumAggregatedValuePoints(
        groupIds.map(
          (groupId) =>
            rowsByWeek.get(weekStartDate)?.[groupId] ?? createEmptyAggregatedValuePoint(),
        ),
      )

      return {
        fecha: weekStartDate,
        label: formatWeekLabel(weekStartDate),
        actual: aggregatedPoint.actual,
        forecast: aggregatedPoint.forecast,
        value2024: aggregatedPoint.value2024,
      }
    })
  }

  const buildChartViews = <TViewId extends ForecastChartViewId>(
    definitions: ChartViewDefinition<TViewId>[],
  ): ForecastChartViewModel[] =>
    definitions.map((definition) => {
      const points = buildPointsForGroups(definition.groupIds)

      return {
        id: definition.id,
        label: definition.label,
        points,
        hasData: points.length > 0,
      }
    })

  return [
    {
      id: 'servicios',
      title: 'Servicios',
      defaultViewId: 'entregas',
      views: buildChartViews(SERVICES_CHART_VIEWS),
    },
    {
      id: 'picking',
      title: 'Picking',
      defaultViewId: 'lineas',
      views: buildChartViews(PICKING_CHART_VIEWS),
    },
  ]
}
