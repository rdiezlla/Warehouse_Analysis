import { lazy, Suspense, useState } from 'react'
import { AlertTriangle, DatabaseZap } from 'lucide-react'

import { Panel } from '@/components/ui/Panel'
import { useForecastData } from '@/features/forecast/hooks/useForecastData'
import {
  buildChartModels,
  buildKpiCards,
  getDefaultSelectedQuarters,
  getAvailableQuarters,
} from '@/features/forecast/utils/forecastSelectors'
import { KpiSummaryCard } from '@/features/forecast/components/KpiSummaryCard'
import { QuarterFilter } from '@/features/forecast/components/QuarterFilter'

const KpiTrendChart = lazy(
  () => import('@/features/forecast/components/KpiTrendChart'),
)

const toggleQuarterSelection = (
  selectedQuarters: number[],
  quarter: number,
): number[] => {
  if (selectedQuarters.includes(quarter)) {
    const nextSelection = selectedQuarters.filter((value) => value !== quarter)
    return nextSelection.length > 0 ? nextSelection : selectedQuarters
  }

  return [...selectedQuarters, quarter].sort((a, b) => a - b)
}

export const ForecastModule = () => {
  const { data, isLoading, error } = useForecastData()
  const [selectedQuarters, setSelectedQuarters] = useState<number[] | null>(null)

  const availableQuarters = getAvailableQuarters()
  const defaultSelectedQuarters = getDefaultSelectedQuarters(
    data?.consumoForecastDiario ?? [],
    data?.consumoVs2024Diario ?? [],
  )
  const effectiveSelectedQuarters = selectedQuarters ?? defaultSelectedQuarters

  if (isLoading) {
    return (
      <Panel className="min-h-[220px] animate-rise">
        <p className="text-sm text-slate-500">Cargando capa de consumo de Forecast...</p>
      </Panel>
    )
  }

  if (error || !data) {
    return (
      <Panel className="min-h-[220px] border-rose-200/70 bg-rose-50/40">
        <div className="flex items-start gap-3 text-rose-700">
          <AlertTriangle size={18} className="mt-0.5 shrink-0" />
          <div>
            <h2 className="text-base font-semibold">No se pudieron cargar los datos</h2>
            <p className="mt-1 text-sm">
              {error ??
                'Falta el contenido de /public/data. Ejecuta npm run sync:data para sincronizar tablas.'}
            </p>
          </div>
        </div>
      </Panel>
    )
  }

  const kpiCards = buildKpiCards(
    data.consumoForecastDiario,
    data.consumoVs2024Diario,
    effectiveSelectedQuarters,
  )
  const chartModels = buildChartModels(
    data.consumoForecastDiario,
    data.consumoVs2024Diario,
    effectiveSelectedQuarters,
  )
  const hasAnyPoint = chartModels.some((model) => model.points.length > 0)

  return (
    <div className="space-y-6">
      <Panel className="animate-rise bg-gradient-to-r from-white via-white to-slate-50">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">
              Forecast
            </p>
            <h1 className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">
              Operativa actual vs forecast vs 2024
            </h1>
            <p className="mt-2 max-w-2xl text-sm text-slate-500">
              Visualización diaria con foco operativo. Los filtros aplican sobre toda la
              página y consolidan KPIs según negocio.
            </p>
          </div>
          <div className="rounded-xl border border-slate-200/70 bg-white/80 px-3 py-2 text-xs text-slate-500">
            <div className="flex items-center gap-2 font-medium text-slate-700">
              <DatabaseZap size={14} />
              Fuente de datos
            </div>
            <p className="mt-1">{data.metadata?.sourcePath ?? 'public/data'}</p>
            <p className="text-[11px] text-slate-400">
              Sincronizado: {data.metadata?.syncedAt ?? 'sin metadata'}
            </p>
          </div>
        </div>

        <div className="mt-5 border-t border-slate-100 pt-4">
          <p className="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500">
            Filtro por trimestre
          </p>
          <QuarterFilter
            availableQuarters={availableQuarters}
            selectedQuarters={effectiveSelectedQuarters}
            onToggleQuarter={(quarter) =>
              setSelectedQuarters((current) =>
                toggleQuarterSelection(current ?? defaultSelectedQuarters, quarter),
              )
            }
          />
        </div>
      </Panel>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {kpiCards.map((card, index) => (
          <div key={card.id} className="animate-rise" style={{ animationDelay: `${index * 70}ms` }}>
            <KpiSummaryCard model={card} />
          </div>
        ))}
      </div>

      {hasAnyPoint ? (
        <div className="grid gap-5 xl:grid-cols-3">
          {chartModels.map((chart) => (
            <Suspense
              key={chart.id}
              fallback={<Panel className="h-[340px] animate-rise" />}
            >
              <KpiTrendChart chart={chart} />
            </Suspense>
          ))}
        </div>
      ) : (
        <Panel className="min-h-[140px]">
          <p className="text-sm text-slate-500">
            No hay puntos para el filtro seleccionado. Activa uno o más trimestres para
            visualizar tarjetas y series.
          </p>
        </Panel>
      )}
    </div>
  )
}
