import { useState } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { Panel } from '@/components/ui/Panel'
import { ChartViewPills } from '@/features/forecast/components/ChartViewPills'
import type { ForecastChartModel } from '@/types/forecast'
import { formatCompactNumber } from '@/utils/formatters'

interface KpiTrendChartProps {
  chart: ForecastChartModel
}

const CHART_PANEL_HEIGHT_CLASS = 'h-[clamp(16.75rem,30vh,20.25rem)]'

const formatAxisTick = (value: number | null): string => formatCompactNumber(value)

const TooltipContent = ({
  active,
  payload,
  label,
}: {
  active?: boolean
  payload?: Array<{ name: string; value: number | null; color: string }>
  label?: string
}) => {
  if (!active || !payload || payload.length === 0) {
    return null
  }

  return (
    <div className="rounded-xl border border-slate-100 bg-white/95 px-3 py-2 shadow-md">
      <p className="mb-1 text-xs font-semibold tracking-wide text-slate-500">{label}</p>
      <div className="space-y-1">
        {payload.map((entry) => (
          <div key={entry.name} className="flex items-center justify-between gap-4 text-xs">
            <span className="font-medium" style={{ color: entry.color }}>
              {entry.name}
            </span>
            <span className="font-semibold text-slate-900">
              {formatCompactNumber(entry.value)}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

const KpiTrendChart = ({ chart }: KpiTrendChartProps) => {
  const [selectedViewId, setSelectedViewId] = useState(chart.defaultViewId)
  const selectedView =
    chart.views.find((view) => view.id === selectedViewId) ?? chart.views[0]

  return (
    <Panel
      className={`flex ${CHART_PANEL_HEIGHT_CLASS} animate-rise flex-col [animation-delay:120ms]`}
    >
      <div className="mb-3 flex flex-col gap-2.5">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
          <h3 className="text-base font-semibold text-slate-900">{chart.title}</h3>
          <p className="text-xs text-slate-400">2024 vs actual vs forecast</p>
        </div>

        <ChartViewPills
          views={chart.views}
          selectedViewId={selectedView.id}
          onSelectView={setSelectedViewId}
        />
      </div>

      {selectedView.hasData ? (
        <div className="min-h-0 flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={selectedView.points}
              margin={{ left: 8, right: 16, top: 8, bottom: 8 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
              <XAxis
                dataKey="label"
                tick={{ fill: '#64748b', fontSize: 12 }}
                axisLine={false}
                tickLine={false}
                minTickGap={18}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: '#64748b', fontSize: 12 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={formatAxisTick}
                tickMargin={8}
                width={80}
              />
              <Tooltip content={<TooltipContent />} />
              <Legend
                wrapperStyle={{
                  fontSize: 12,
                  color: '#475569',
                  paddingTop: 8,
                }}
              />
              <Line
                type="monotone"
                dataKey="value2024"
                name="2024"
                stroke="var(--line-2024)"
                strokeWidth={2}
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="actual"
                name="Actual"
                stroke="var(--line-actual)"
                strokeWidth={2.5}
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="forecast"
                name="Forecast"
                stroke="var(--line-forecast)"
                strokeWidth={2}
                strokeDasharray="6 6"
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div className="flex flex-1 items-center justify-center rounded-2xl border border-dashed border-slate-200 bg-slate-50/60 px-4 text-center">
          <p className="text-sm text-slate-500">
            No hay puntos para esta vista con el filtro seleccionado.
          </p>
        </div>
      )}
    </Panel>
  )
}

export default KpiTrendChart
