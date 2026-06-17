import type { RackSlot } from '@/features/warehouse3d/types'
import { WAREHOUSE_ZONES } from '@/features/warehouse3d/layout/warehouseLayout'

interface LocationInspectorProps {
  selectedSlot: RackSlot | null
  onClearSelection: () => void
}

const getZoneLabel = (zoneId: string) =>
  WAREHOUSE_ZONES.find((zone) => zone.id === zoneId)?.label ?? zoneId

export const LocationInspector = ({
  selectedSlot,
  onClearSelection,
}: LocationInspectorProps) => {
  if (!selectedSlot) {
    return (
      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 p-4 text-sm text-slate-500">
        Selecciona un hueco del rack para ver su informacion.
      </div>
    )
  }

  const rows = [
    ['ID', selectedSlot.id],
    ['Zona', getZoneLabel(selectedSlot.zoneId)],
    ['Pasillo', `P${String(selectedSlot.aisle).padStart(2, '0')}`],
    ['Lado', selectedSlot.side],
    ['Modulo', `M${String(selectedSlot.moduleIndex).padStart(2, '0')}`],
    [
      'Ubicaciones',
      `${String(selectedSlot.startLocation).padStart(3, '0')} - ${String(
        selectedSlot.endLocation,
      ).padStart(3, '0')}`,
    ],
    ['Altura', String(selectedSlot.level)],
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
