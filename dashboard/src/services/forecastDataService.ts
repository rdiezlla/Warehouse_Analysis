import { fetchJson, toNullableNumber } from '@/services/dataClient'
import type {
  ConsumoForecastDiarioRow,
  ConsumoForecastSemanalRow,
  ConsumoProgresoActualRow,
  ConsumoVs2024DiarioRow,
  ConsumoVs2024SemanalRow,
  DashboardDataMetadata,
  DimKpiRow,
  ForecastDashboardData,
} from '@/types/forecast'

const DATA_BASE_PATH = '/data'

const tablePath = (tableName: string): string => `${DATA_BASE_PATH}/${tableName}.json`

const mapConsumoForecastDiario = (
  rows: Array<Record<string, unknown>>,
): ConsumoForecastDiarioRow[] =>
  rows.map((row) => ({
    fecha: String(row.fecha ?? ''),
    kpi: String(row.kpi ?? '') as ConsumoForecastDiarioRow['kpi'],
    forecast_value: toNullableNumber(row.forecast_value),
    actual_value: toNullableNumber(row.actual_value),
    actual_truth_status: row.actual_truth_status
      ? String(row.actual_truth_status)
      : undefined,
  }))

const mapConsumoForecastSemanal = (
  rows: Array<Record<string, unknown>>,
): ConsumoForecastSemanalRow[] =>
  rows.map((row) => ({
    week_start_date: String(row.week_start_date ?? ''),
    kpi: String(row.kpi ?? '') as ConsumoForecastSemanalRow['kpi'],
    forecast_value: toNullableNumber(row.forecast_value),
    actual_value: toNullableNumber(row.actual_value),
  }))

const mapConsumoVs2024Diario = (
  rows: Array<Record<string, unknown>>,
): ConsumoVs2024DiarioRow[] =>
  rows.map((row) => ({
    fecha: String(row.fecha ?? ''),
    kpi: String(row.kpi ?? '') as ConsumoVs2024DiarioRow['kpi'],
    actual_value_current: toNullableNumber(row.actual_value_current),
    actual_value_2024: toNullableNumber(row.actual_value_2024),
  }))

const mapConsumoVs2024Semanal = (
  rows: Array<Record<string, unknown>>,
): ConsumoVs2024SemanalRow[] =>
  rows.map((row) => ({
    week_start_date: String(row.week_start_date ?? ''),
    kpi: String(row.kpi ?? '') as ConsumoVs2024SemanalRow['kpi'],
    actual_value_current: toNullableNumber(row.actual_value_current),
    actual_value_2024: toNullableNumber(row.actual_value_2024),
  }))

const mapConsumoProgresoActual = (
  rows: Array<Record<string, unknown>>,
): ConsumoProgresoActualRow[] =>
  rows.map((row) => ({
    fecha_corte: String(row.fecha_corte ?? ''),
    tipo_periodo: String(row.tipo_periodo ?? 'day') as ConsumoProgresoActualRow['tipo_periodo'],
    kpi: String(row.kpi ?? '') as ConsumoProgresoActualRow['kpi'],
    forecast_total_periodo: toNullableNumber(row.forecast_total_periodo),
    actual_acumulado_hasta_hoy: toNullableNumber(row.actual_acumulado_hasta_hoy),
    actual_2024_acumulado: toNullableNumber(row.actual_2024_acumulado),
    diff_pct_vs_2024_acumulado: toNullableNumber(row.diff_pct_vs_2024_acumulado),
  }))

const mapDimKpi = (rows: Array<Record<string, unknown>>): DimKpiRow[] =>
  rows.map((row) => ({
    kpi: String(row.kpi ?? '') as DimKpiRow['kpi'],
    nombre_negocio: String(row.nombre_negocio ?? ''),
    unidad: String(row.unidad ?? ''),
    nivel_uso_operativo: String(row.nivel_uso_operativo ?? ''),
    orden_visual: Number(row.orden_visual ?? 0),
  }))

export const loadForecastDashboardData = async (): Promise<ForecastDashboardData> => {
  const [
    consumoForecastDiarioRaw,
    consumoForecastSemanalRaw,
    consumoVs2024DiarioRaw,
    consumoVs2024SemanalRaw,
    consumoProgresoActualRaw,
    dimKpiRaw,
    metadata,
  ] = await Promise.all([
    fetchJson<Array<Record<string, unknown>>>(tablePath('consumo_forecast_diario')).catch(
      () => [],
    ),
    fetchJson<Array<Record<string, unknown>>>(tablePath('consumo_forecast_semanal')).catch(
      () => [],
    ),
    fetchJson<Array<Record<string, unknown>>>(tablePath('consumo_vs_2024_diario')).catch(
      () => [],
    ),
    fetchJson<Array<Record<string, unknown>>>(tablePath('consumo_vs_2024_semanal')).catch(
      () => [],
    ),
    fetchJson<Array<Record<string, unknown>>>(tablePath('consumo_progreso_actual')).catch(
      () => [],
    ),
    fetchJson<Array<Record<string, unknown>>>(tablePath('dim_kpi')).catch(() => []),
    fetchJson<DashboardDataMetadata>(tablePath('_metadata')).catch(() => null),
  ])

  return {
    consumoForecastDiario: mapConsumoForecastDiario(consumoForecastDiarioRaw),
    consumoForecastSemanal: mapConsumoForecastSemanal(consumoForecastSemanalRaw),
    consumoVs2024Diario: mapConsumoVs2024Diario(consumoVs2024DiarioRaw),
    consumoVs2024Semanal: mapConsumoVs2024Semanal(consumoVs2024SemanalRaw),
    consumoProgresoActual: mapConsumoProgresoActual(consumoProgresoActualRaw),
    dimKpi: mapDimKpi(dimKpiRaw),
    metadata,
  }
}
