import clsx from 'clsx'

import type { ForecastChartViewId, ForecastChartViewModel } from '@/types/forecast'

interface ChartViewPillsProps {
  views: ForecastChartViewModel[]
  selectedViewId: ForecastChartViewId
  onSelectView: (viewId: ForecastChartViewId) => void
}

export const ChartViewPills = ({
  views,
  selectedViewId,
  onSelectView,
}: ChartViewPillsProps) => (
  <div className="flex flex-wrap gap-2">
    {views.map((view) => {
      const isActive = view.id === selectedViewId

      return (
        <button
          key={view.id}
          type="button"
          onClick={() => onSelectView(view.id)}
          className={clsx(
            'rounded-full border px-3.5 py-1.5 text-xs font-medium transition-all duration-200',
            isActive
              ? 'border-slate-900 bg-slate-900 text-white shadow-sm shadow-slate-200/80'
              : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-900',
          )}
        >
          {view.label}
        </button>
      )
    })}
  </div>
)
