import clsx from 'clsx'
import { Activity, Boxes, CalendarDays, ChartLine, ShieldCheck } from 'lucide-react'

const menuItems = [
  { label: 'Forecast', icon: ChartLine, active: true },
  { label: 'Capacity', icon: Boxes, active: false },
  { label: 'Workforce', icon: Activity, active: false },
  { label: 'Calendar', icon: CalendarDays, active: false },
  { label: 'Quality', icon: ShieldCheck, active: false },
]

export const Sidebar = () => (
  <aside className="relative flex w-full shrink-0 flex-col border-b border-white/60 bg-white/80 p-4 backdrop-blur lg:w-72 lg:border-b-0 lg:border-r">
    <div className="mb-6 flex items-center gap-3">
      <div className="relative h-10 w-10 shrink-0 rounded-xl bg-slate-900 p-2">
        <div className="absolute left-2 top-2 h-2.5 w-2.5 rounded-sm bg-cyan-300" />
        <div className="absolute right-2 top-2 h-2.5 w-2.5 rounded-sm bg-white/70" />
        <div className="absolute bottom-2 left-2 h-2.5 w-2.5 rounded-sm bg-white/70" />
        <div className="absolute bottom-2 right-2 h-2.5 w-2.5 rounded-sm bg-cyan-300" />
      </div>
      <div>
        <p className="text-sm font-semibold tracking-tight text-slate-900">
          Warehouse Analysis
        </p>
        <p className="text-xs text-slate-500">Operational Intelligence</p>
      </div>
    </div>

    <nav className="flex flex-col gap-1">
      {menuItems.map((item) => {
        const Icon = item.icon
        return (
          <button
            key={item.label}
            type="button"
            disabled={!item.active}
            className={clsx(
              'flex items-center gap-3 rounded-xl px-3 py-2 text-left text-sm transition',
              item.active
                ? 'bg-slate-900 text-white shadow-sm'
                : 'text-slate-400 hover:bg-white hover:text-slate-600 disabled:cursor-not-allowed',
            )}
          >
            <Icon size={16} />
            <span className="font-medium">{item.label}</span>
            {!item.active && (
              <span className="ml-auto rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-400">
                Soon
              </span>
            )}
          </button>
        )
      })}
    </nav>
  </aside>
)
