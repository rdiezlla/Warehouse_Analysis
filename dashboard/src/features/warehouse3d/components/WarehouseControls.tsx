import { RotateCcw } from 'lucide-react'

import type { RackBayType } from '@/features/warehouse3d/types'

interface WarehouseControlsProps {
  showLabels: boolean
  showReferenceZones: boolean
  editRackTypes: boolean
  selectedRackType: RackBayType
  hasRackTypeOverrides: boolean
  onShowLabelsChange: (showLabels: boolean) => void
  onShowReferenceZonesChange: (showReferenceZones: boolean) => void
  onEditRackTypesChange: (editRackTypes: boolean) => void
  onSelectedRackTypeChange: (rackType: RackBayType) => void
  onResetRackTypes: () => void
}

export const WarehouseControls = ({
  showLabels,
  showReferenceZones,
  editRackTypes,
  selectedRackType,
  hasRackTypeOverrides,
  onShowLabelsChange,
  onShowReferenceZonesChange,
  onEditRackTypesChange,
  onSelectedRackTypeChange,
  onResetRackTypes,
}: WarehouseControlsProps) => (
  <div className="space-y-5">
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

    <div className="space-y-3 border-t border-slate-200 pt-4">
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
        Tipo de rack
      </p>

      <label className="flex items-center gap-3 text-sm font-medium text-slate-700">
        <input
          type="checkbox"
          checked={editRackTypes}
          onChange={(event) => onEditRackTypesChange(event.target.checked)}
          className="h-4 w-4 rounded border-slate-300 text-cyan-600 focus:ring-cyan-500"
        />
        Editar tipos de rack
      </label>

      <div className="grid grid-cols-2 overflow-hidden rounded-lg border border-slate-200 bg-slate-50 p-1">
        {([
          ['standard-3eu', 'Rack 3 EU'],
          ['split-6eu', 'Rack 6 EU'],
        ] as const).map(([type, label]) => (
          <button
            key={type}
            type="button"
            onClick={() => onSelectedRackTypeChange(type)}
            className={`rounded-md px-2 py-2 text-xs font-semibold transition ${
              selectedRackType === type
                ? 'bg-slate-900 text-white shadow-sm'
                : 'text-slate-600 hover:bg-white hover:text-slate-900'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      <p className="text-xs leading-5 text-slate-500">
        Selecciona un tipo de rack y haz click sobre un vano del mapa para cambiarlo.
      </p>

      <button
        type="button"
        onClick={onResetRackTypes}
        disabled={!hasRackTypeOverrides}
        className="flex w-full items-center justify-center gap-2 rounded-lg border border-slate-200 px-3 py-2 text-xs font-semibold text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-45"
      >
        <RotateCcw size={14} />
        Restablecer tipos de rack
      </button>
    </div>
  </div>
)
