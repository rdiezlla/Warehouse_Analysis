import type { RackBayType, RackLocation } from '@/features/warehouse3d/types'
import { WAREHOUSE_ZONES } from '@/features/warehouse3d/layout/warehouseLayout'

interface LocationInspectorProps {
  selectedLocation: RackLocation | null
  selectedRackType: RackBayType | null
  onClearSelection: () => void
}

const getZoneLabel = (zoneId: string) =>
  WAREHOUSE_ZONES.find((zone) => zone.id === zoneId)?.label ?? zoneId

export const LocationInspector = ({
  selectedLocation,
  selectedRackType,
  onClearSelection,
}: LocationInspectorProps) => {
  if (!selectedLocation) {
    return (
      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 p-4 text-sm text-slate-500">
        Selecciona una ubicacion del rack para ver su informacion.
      </div>
    )
  }

  const rows = [
    ['ID', selectedLocation.id],
    ['Zona', getZoneLabel(selectedLocation.zoneId)],
    ['Pasillo', String(selectedLocation.aisle).padStart(2, '0')],
    ['Lado', selectedLocation.side],
    ['Ubicacion', String(selectedLocation.location).padStart(3, '0')],
    ['Nivel', String(selectedLocation.level).padStart(2, '0')],
    ['Vano', String(selectedLocation.bayIndex)],
    ['Posicion', `${selectedLocation.positionInsideBay} de 3`],
    ['Tipo de rack', selectedRackType === 'split-6eu' ? 'Rack 6 EU' : 'Rack 3 EU'],
    ['Estado', 'Vacio'],
  ]

  return (
    <div className="space-y-4">
      <dl className="divide-y divide-slate-100 rounded-xl border border-slate-200 bg-white">
        {rows.map(([label, value]) => (
          <div key={label} className="grid grid-cols-[6rem_minmax(0,1fr)] gap-3 px-3 py-2.5">
            <dt className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {label}
            </dt>
            <dd className="break-words text-sm font-medium text-slate-800">{value}</dd>
          </div>
        ))}
      </dl>

      <button
        type="button"
        onClick={onClearSelection}
        className="w-full rounded-xl bg-slate-900 px-3 py-2 text-sm font-semibold text-white transition hover:bg-slate-700"
      >
        Limpiar seleccion
      </button>
    </div>
  )
}
