export type KpiCode =
  | 'entregas_sge'
  | 'entregas_sgp'
  | 'recogidas_ege'
  | 'picking_lines_pi'
  | 'picking_units_pi'

export type KpiGroupId =
  | 'entregas'
  | 'recogidas'
  | 'lineas_preparadas'
  | 'unidades_preparadas'

export interface ConsumoForecastDiarioRow {
  fecha: string
  kpi: KpiCode
  forecast_value: number | null
  actual_value: number | null
  actual_truth_status?: string
}

export interface ConsumoForecastSemanalRow {
  week_start_date: string
  kpi: KpiCode
  forecast_value: number | null
  actual_value: number | null
}

export interface ConsumoVs2024DiarioRow {
  fecha: string
  kpi: KpiCode
  actual_value_current: number | null
  actual_value_2024: number | null
}

export interface ConsumoVs2024SemanalRow {
  week_start_date: string
  kpi: KpiCode
  actual_value_current: number | null
  actual_value_2024: number | null
}

export interface ConsumoProgresoActualRow {
  fecha_corte: string
  tipo_periodo: 'day' | 'week' | 'month'
  kpi: KpiCode
  forecast_total_periodo: number | null
  actual_acumulado_hasta_hoy: number | null
  actual_2024_acumulado: number | null
  diff_pct_vs_2024_acumulado: number | null
}

export interface DimKpiRow {
  kpi: KpiCode
  nombre_negocio: string
  unidad: string
  nivel_uso_operativo: string
  orden_visual: number
}

export interface DashboardDataMetadata {
  sourceMode: 'consumption_outputs' | 'demo'
  sourcePath: string
  syncedAt: string
  tables: Record<string, number>
}

export interface ForecastDashboardData {
  consumoForecastDiario: ConsumoForecastDiarioRow[]
  consumoForecastSemanal: ConsumoForecastSemanalRow[]
  consumoVs2024Diario: ConsumoVs2024DiarioRow[]
  consumoVs2024Semanal: ConsumoVs2024SemanalRow[]
  consumoProgresoActual: ConsumoProgresoActualRow[]
  dimKpi: DimKpiRow[]
  metadata: DashboardDataMetadata | null
}

export interface KpiGroupDefinition {
  id: KpiGroupId
  label: string
  kpis: KpiCode[]
  accentColor: string
}

export interface KpiCardModel {
  id: KpiGroupId
  title: string
  valueActual: number | null
  valueForecast: number | null
  value2024: number | null
  headlineValue: number | null
  headlineLabel: string
  deltaVsForecastPct: number | null
  deltaVs2024Pct: number | null
  hasData: boolean
}

export interface ForecastChartPoint {
  fecha: string
  label: string
  actual: number | null
  forecast: number | null
  value2024: number | null
}

export interface ForecastChartModel {
  id: KpiGroupId
  title: string
  points: ForecastChartPoint[]
}
