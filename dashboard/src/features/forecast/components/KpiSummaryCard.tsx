import { ArrowDownRight, ArrowUpRight } from 'lucide-react'
import clsx from 'clsx'

import type { KpiCardModel } from '@/types/forecast'
import { formatCompactNumber, formatPercent } from '@/utils/formatters'

interface KpiSummaryCardProps {
  model: KpiCardModel
}

const renderMetricValue = (
  value: number | null,
  emptyLabel: string,
): string => (value === null || Number.isNaN(value) ? emptyLabel : formatCompactNumber(value))

const DeltaIndicator = ({ value }: { value: number | null }) => {
  if (value === null) {
    return <span className="text-xs text-slate-400">sin referencia</span>
  }

  const isPositive = value >= 0
  const Icon = isPositive ? ArrowUpRight : ArrowDownRight
  return (
    <span
      className={clsx(
        'inline-flex items-center gap-1 rounded-full px-2 py-1 text-xs font-semibold',
        isPositive
          ? 'bg-emerald-50 text-emerald-700'
          : 'bg-rose-50 text-rose-700',
      )}
    >
      <Icon size={14} />
      {formatPercent(value)}
    </span>
  )
}

export const KpiSummaryCard = ({ model }: KpiSummaryCardProps) => (
  <article className="group rounded-2xl border border-white/60 bg-white/95 p-4 shadow-[0_12px_28px_-20px_rgba(15,23,42,0.45)] transition hover:-translate-y-0.5 hover:shadow-[0_16px_30px_-18px_rgba(15,23,42,0.3)]">
    <div className="mb-1.5 flex items-center justify-between">
      <p className="text-sm font-medium text-slate-500">{model.title}</p>
      <DeltaIndicator value={model.deltaVsForecastPct} />
    </div>
    <p className="text-[1.75rem] font-semibold leading-none tracking-tight text-slate-950">
      {formatCompactNumber(model.headlineValue)}
    </p>
    <p className="mt-1 text-xs font-medium uppercase tracking-wide text-slate-400">
      {model.headlineLabel}
    </p>
    <div className="mt-3 space-y-1.5 text-sm">
      <div className="flex items-center justify-between text-slate-500">
        <span>Forecast</span>
        <span className="font-medium text-slate-700">
          {renderMetricValue(model.valueForecast, 'sin forecast')}
        </span>
      </div>
      <div className="flex items-center justify-between text-slate-500">
        <span>2024</span>
        <span className="font-medium text-slate-700">
          {renderMetricValue(model.value2024, 'sin referencia')}
        </span>
      </div>
      <div className="mt-2.5 flex items-center justify-between border-t border-slate-100 pt-2.5">
        <span className="text-xs text-slate-400">vs 2024</span>
        <DeltaIndicator value={model.deltaVs2024Pct} />
      </div>
    </div>
  </article>
)
