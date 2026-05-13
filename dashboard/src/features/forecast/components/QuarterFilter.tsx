import clsx from 'clsx'

interface QuarterFilterProps {
  availableQuarters: number[]
  selectedQuarters: number[]
  onToggleQuarter: (quarter: number) => void
}

export const QuarterFilter = ({
  availableQuarters,
  selectedQuarters,
  onToggleQuarter,
}: QuarterFilterProps) => (
  <div className="flex flex-wrap gap-2">
    {availableQuarters.map((quarter) => {
      const isActive = selectedQuarters.includes(quarter)
      return (
        <button
          key={quarter}
          type="button"
          onClick={() => onToggleQuarter(quarter)}
          className={clsx(
            'rounded-full border px-4 py-2 text-sm font-medium transition',
            isActive
              ? 'border-slate-900 bg-slate-900 text-white shadow-sm'
              : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-900',
          )}
        >
          Q{quarter}
        </button>
      )
    })}
  </div>
)
