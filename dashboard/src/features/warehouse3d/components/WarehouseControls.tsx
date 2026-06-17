import type { RackLevel, RackLevelFilter } from '@/features/warehouse3d/types'
import { RACK_LEVELS } from '@/features/warehouse3d/layout/warehouseLayout'

interface WarehouseControlsProps {
  selectedLevel: RackLevelFilter
  showLabels: boolean
  showReferenceZones: boolean
  onSelectedLevelChange: (level: RackLevelFilter) => void
  onShowLabelsChange: (showLabels: boolean) => void
  onShowReferenceZonesChange: (showReferenceZones: boolean) => void
}

export const WarehouseControls = ({
  selectedLevel,
  showLabels,
  showReferenceZones,
  onSelectedLevelChange,
  onShowLabelsChange,
  onShowReferenceZonesChange,
}: WarehouseControlsProps) => (
  <div className="space-y-5">
    <div>
      <label
        htmlFor="warehouse-level-filter"
        className="mb-2 block text-xs font-semibold uppercase tracking-wide text-slate-500"
      >
        Altura visible
      </label>
      <select
        id="warehouse-level-filter"
        value={selectedLevel}
        onChange={(event) => {
          const value = event.target.value
          onSelectedLevelChange(value === 'all' ? 'all' : (Number(value) as RackLevel))
        }}
        className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm outline-none transition focus:border-cyan-500 focus:ring-2 focus:ring-cyan-100"
      >
        <option value="all">Todas</option>
        {RACK_LEVELS.map((level) => (
          <option key={level} value={level}>
            {level}
          </option>
        ))}
      </select>
    </div>

    <label className="flex items-center gap-3 text-sm font-medium text-slate-700">
      <input
        type="checkbox"
        checked={showLabels}
        onChange={(event) => onShowLabelsChange(event.target.checked)}
        className="h-4 w-4 rounded border-slate-300 text-cyan-600 focus:ring-cyan-500"
      />
      Mostrar etiquetas
    </label>

    <label className="flex items-center gap-3 text-sm font-medium text-slate-700">
      <input
        type="checkbox"
        checked={showReferenceZones}
        onChange={(event) => onShowReferenceZonesChange(event.target.checked)}
        className="h-4 w-4 rounded border-slate-300 text-cyan-600 focus:ring-cyan-500"
      />
      Mostrar zonas de referencia
    </label>
  </div>
)
