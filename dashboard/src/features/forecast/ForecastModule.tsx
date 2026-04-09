import { lazy, Suspense, useState } from 'react'
import { AlertTriangle } from 'lucide-react'

import { Panel } from '@/components/ui/Panel'
import { QuarterFilter } from '@/features/forecast/components/QuarterFilter'
import { KpiSummaryCard } from '@/features/forecast/components/KpiSummaryCard'
import { useForecastData } from '@/features/forecast/hooks/useForecastData'
import {
  buildKpiCards,
  buildWeeklyChartModels,
  getAvailableQuarters,
  getDefaultSelectedQuarters,
} from '@/features/forecast/utils/forecastSelectors'

const KpiTrendChart = lazy(
  () => import('@/features/forecast/components/KpiTrendChart'),
)

const CHART_PANEL_HEIGHT_CLASS = 'h-[clamp(16.75rem,30vh,20.25rem)]'

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
  const chartModels = buildWeeklyChartModels(
    data.consumoForecastSemanal,
    data.consumoVs2024Semanal,
    effectiveSelectedQuarters,
  )
  const hasAnyPoint = chartModels.some((model) =>
    model.views.some((view) => view.hasData),
  )

  return (
    <div className="space-y-5">
      <Panel className="animate-rise bg-gradient-to-r from-white via-white to-slate-50">
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
      </Panel>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {kpiCards.map((card, index) => (
          <div
            key={card.id}
            className="animate-rise"
            style={{ animationDelay: `${index * 70}ms` }}
          >
            <KpiSummaryCard model={card} />
          </div>
        ))}
      </div>

      {hasAnyPoint ? (
        <div className="grid gap-4">
          {chartModels.map((chart) => (
            <Suspense
              key={chart.id}
              fallback={<Panel className={`${CHART_PANEL_HEIGHT_CLASS} animate-rise`} />}
            >
              <KpiTrendChart chart={chart} />
            </Suspense>
          ))}
        </div>
      ) : (
        <Panel className="min-h-[140px]">
          <p className="text-sm text-slate-500">
            No hay puntos para el filtro seleccionado. Activa uno o mas trimestres para
            visualizar tarjetas y series.
          </p>
        </Panel>
      )}
    </div>
  )
}
