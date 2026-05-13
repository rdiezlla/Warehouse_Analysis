import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { parse } from 'csv-parse/sync'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const dashboardRoot = path.resolve(__dirname, '..')
const repositoryRoot = path.resolve(dashboardRoot, '..')
const outputDataDirectory = path.join(dashboardRoot, 'public', 'data')
const sourceConsumptionDirectory =
  process.env.WA_CONSUMPTION_DIR ??
  path.join(repositoryRoot, 'outputs', 'consumption')

const TABLES = [
  'consumo_forecast_diario',
  'consumo_forecast_semanal',
  'consumo_vs_2024_diario',
  'consumo_vs_2024_semanal',
  'consumo_progreso_actual',
  'dim_kpi',
]

const REQUIRED_TABLES = [
  'consumo_forecast_diario',
  'consumo_vs_2024_diario',
  'dim_kpi',
]

const KPI_CONFIG = [
  { kpi: 'entregas_sge', base: 145, volatility: 0.12 },
  { kpi: 'entregas_sgp', base: 116, volatility: 0.11 },
  { kpi: 'recogidas_ege', base: 88, volatility: 0.13 },
  { kpi: 'picking_lines_pi', base: 1890, volatility: 0.08 },
  { kpi: 'picking_units_pi', base: 27850, volatility: 0.09 },
]

const DIM_KPI_DEMO = [
  {
    kpi: 'entregas_sge',
    nombre_negocio: 'Entregas SGE',
    unidad: 'servicios',
    nivel_uso_operativo: 'operativo',
    orden_visual: 1,
  },
  {
    kpi: 'entregas_sgp',
    nombre_negocio: 'Entregas SGP',
    unidad: 'servicios',
    nivel_uso_operativo: 'operativo',
    orden_visual: 2,
  },
  {
    kpi: 'recogidas_ege',
    nombre_negocio: 'Recogidas EGE',
    unidad: 'servicios',
    nivel_uso_operativo: 'cautela',
    orden_visual: 3,
  },
  {
    kpi: 'picking_lines_pi',
    nombre_negocio: 'Picking líneas PI',
    unidad: 'lineas',
    nivel_uso_operativo: 'operativo',
    orden_visual: 4,
  },
  {
    kpi: 'picking_units_pi',
    nombre_negocio: 'Picking unidades PI',
    unidad: 'unidades',
    nivel_uso_operativo: 'operativo',
    orden_visual: 5,
  },
]

const toIsoDate = (date) => date.toISOString().slice(0, 10)

const startOfIsoWeek = (date) => {
  const copy = new Date(date)
  const day = copy.getDay() || 7
  copy.setDate(copy.getDate() - day + 1)
  copy.setHours(0, 0, 0, 0)
  return copy
}

const roundMetric = (value) => Math.max(0, Math.round(value))

const parseCsv = (text) =>
  parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  })

const pathExists = async (targetPath) => {
  try {
    await fs.access(targetPath)
    return true
  } catch {
    return false
  }
}

const readTable = async (tableName) => {
  const jsonPath = path.join(sourceConsumptionDirectory, `${tableName}.json`)
  if (await pathExists(jsonPath)) {
    const content = await fs.readFile(jsonPath, 'utf8')
    return JSON.parse(content)
  }

  const csvPath = path.join(sourceConsumptionDirectory, `${tableName}.csv`)
  if (await pathExists(csvPath)) {
    const content = await fs.readFile(csvPath, 'utf8')
    return parseCsv(content)
  }

  return null
}

const aggregateByWeek = (rows, dateColumnName, valueColumns) => {
  const map = new Map()
  for (const row of rows) {
    const date = new Date(row[dateColumnName])
    const weekStart = toIsoDate(startOfIsoWeek(date))
    const key = `${weekStart}__${row.kpi}`
    const current = map.get(key) ?? {
      week_start_date: weekStart,
      kpi: row.kpi,
      ...Object.fromEntries(valueColumns.map((column) => [column, 0])),
    }

    for (const column of valueColumns) {
      const value = Number(row[column] ?? 0)
      current[column] += Number.isFinite(value) ? value : 0
    }
    map.set(key, current)
  }
  return [...map.values()]
}

const createDemoTables = () => {
  const dailyRows = []
  const vsRows = []
  const startDate = new Date('2026-01-01T00:00:00Z')
  const totalDays = 365

  for (let dayIndex = 0; dayIndex < totalDays; dayIndex += 1) {
    const date = new Date(startDate)
    date.setDate(startDate.getDate() + dayIndex)
    const dateLabel = toIsoDate(date)

    for (const [kpiIndex, kpi] of KPI_CONFIG.entries()) {
      const seasonal =
        1 +
        Math.sin((2 * Math.PI * dayIndex) / 30 + kpiIndex * 0.4) *
          kpi.volatility
      const trend = 1 + dayIndex * 0.0005
      const weekendFactor = [0, 6].includes(date.getDay()) ? 0.84 : 1
      const forecast = roundMetric(kpi.base * seasonal * trend * weekendFactor)
      const actual = roundMetric(
        forecast *
          (0.97 +
            Math.sin((2 * Math.PI * dayIndex) / 17 + kpiIndex * 0.75) * 0.05),
      )
      const reference2024 = roundMetric(
        forecast *
          (0.92 +
            Math.cos((2 * Math.PI * dayIndex) / 28 + kpiIndex * 0.7) * 0.04),
      )

      dailyRows.push({
        fecha: dateLabel,
        kpi: kpi.kpi,
        forecast_value: forecast,
        actual_value: actual,
        actual_truth_status: dayIndex < 220 ? 'observado_final' : 'visible_cartera',
      })

      vsRows.push({
        fecha: dateLabel,
        kpi: kpi.kpi,
        actual_value_current: actual,
        actual_value_2024: reference2024,
      })
    }
  }

  const weeklyForecast = aggregateByWeek(dailyRows, 'fecha', [
    'forecast_value',
    'actual_value',
  ])
  const weeklyVs2024 = aggregateByWeek(vsRows, 'fecha', [
    'actual_value_current',
    'actual_value_2024',
  ])
  const progressDate = dailyRows[dailyRows.length - 1].fecha
  const progressRows = ['day', 'week', 'month'].flatMap((period) =>
    KPI_CONFIG.map((kpi) => ({
      fecha_corte: progressDate,
      tipo_periodo: period,
      kpi: kpi.kpi,
      forecast_total_periodo: roundMetric(kpi.base * (period === 'day' ? 1 : period === 'week' ? 7 : 30)),
      actual_acumulado_hasta_hoy: roundMetric(kpi.base * (period === 'day' ? 1 : period === 'week' ? 6.7 : 27.5)),
      actual_2024_acumulado: roundMetric(kpi.base * (period === 'day' ? 0.95 : period === 'week' ? 6.2 : 25.4)),
      diff_pct_vs_2024_acumulado: 0.05,
    })),
  )

  return {
    consumo_forecast_diario: dailyRows,
    consumo_forecast_semanal: weeklyForecast,
    consumo_vs_2024_diario: vsRows,
    consumo_vs_2024_semanal: weeklyVs2024,
    consumo_progreso_actual: progressRows,
    dim_kpi: DIM_KPI_DEMO,
  }
}

const writeJson = async (outputPath, payload) => {
  await fs.writeFile(outputPath, JSON.stringify(payload, null, 2), 'utf8')
}

const run = async () => {
  await fs.mkdir(outputDataDirectory, { recursive: true })

  const tableEntries = await Promise.all(
    REQUIRED_TABLES.map(async (tableName) => [tableName, await readTable(tableName)]),
  )
  const hasRealData = tableEntries.every(([, rows]) => Array.isArray(rows) && rows.length > 0)

  const sourceTables = hasRealData
    ? Object.fromEntries(
        await Promise.all(
          TABLES.map(async (tableName) => {
            const rows = await readTable(tableName)
            return [tableName, Array.isArray(rows) ? rows : []]
          }),
        ),
      )
    : createDemoTables()

  for (const tableName of TABLES) {
    const rows = sourceTables[tableName] ?? []
    await writeJson(path.join(outputDataDirectory, `${tableName}.json`), rows)
  }

  const metadata = {
    sourceMode: hasRealData ? 'consumption_outputs' : 'demo',
    sourcePath: hasRealData
      ? sourceConsumptionDirectory
      : 'demo_embebido (outputs/consumption no encontrado o incompleto)',
    syncedAt: new Date().toISOString(),
    tables: Object.fromEntries(
      TABLES.map((tableName) => [tableName, sourceTables[tableName]?.length ?? 0]),
    ),
  }

  await writeJson(path.join(outputDataDirectory, '_metadata.json'), metadata)

  const infoLine = hasRealData
    ? `Datos sincronizados desde ${sourceConsumptionDirectory}`
    : 'No se encontró capa de consumo completa; se generó dataset demo.'
  console.log(infoLine)
  console.log(`Salida actualizada en ${outputDataDirectory}`)
}

run().catch((error) => {
  console.error('Error en sync:data:', error)
  process.exitCode = 1
})
