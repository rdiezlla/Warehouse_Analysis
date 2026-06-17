import { useMemo, useState } from 'react'

import { Panel } from '@/components/ui/Panel'
import { LocationInspector } from '@/features/warehouse3d/components/LocationInspector'
import { WarehouseControls } from '@/features/warehouse3d/components/WarehouseControls'
import { WarehouseLegend } from '@/features/warehouse3d/components/WarehouseLegend'
import { WarehouseScene } from '@/features/warehouse3d/components/WarehouseScene'
import {
  LOCATIONS_PER_MODULE,
  WAREHOUSE_SLOTS,
  WAREHOUSE_ZONES,
} from '@/features/warehouse3d/layout/warehouseLayout'
import type { RackLevelFilter, RackSlot } from '@/features/warehouse3d/types'

export const Warehouse3DModule = () => {
  const [selectedLevel, setSelectedLevel] = useState<RackLevelFilter>('all')
  const [showLabels, setShowLabels] = useState(true)
  const [showReferenceZones, setShowReferenceZones] = useState(true)
  const [selectedSlot, setSelectedSlot] = useState<RackSlot | null>(null)

  const visibleSlots = useMemo(
    () =>
      selectedLevel === 'all'
        ? WAREHOUSE_SLOTS
        : WAREHOUSE_SLOTS.filter((slot) => slot.level === selectedLevel),
    [selectedLevel],
  )

  const stats = useMemo(() => {
    const zoneSummaries = WAREHOUSE_ZONES.map((zone) => {
      const modulesPerFace =
        (zone.endLocation - zone.startLocation + 1) / LOCATIONS_PER_MODULE

      return {
        id: zone.id,
        label: zone.label,
        faces: zone.faces.length,
        modulesPerFace,
      }
    })

    return {
      slotCount: WAREHOUSE_SLOTS.length,
      zoneSummaries,
    }
  }, [])

  return (
    <div className="space-y-5">
      <div className="animate-rise">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-700">
          Proyecto de traslado
        </p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight text-slate-950">
          Layout 3D - Traslado Villaverde
        </h1>
        <p className="mt-2 max-w-3xl text-sm text-slate-500">
          Representacion inicial de racks vacios por pasillo, lado, modulo y altura.
        </p>
      </div>

      <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_22rem]">
        <Panel className="animate-rise overflow-hidden p-0">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-100 px-5 py-4">
            <div>
              <h2 className="text-base font-semibold text-slate-900">
                Almacen 3D vacio
              </h2>
              <p className="mt-1 text-xs text-slate-500">
                Zona A: 14 modulos desde U037. Zona B: 20 modulos desde U001.
              </p>
            </div>
            <div className="text-right text-xs font-medium text-slate-500">
              {visibleSlots.length} huecos visibles de {stats.slotCount}
            </div>
          </div>

          <div className="h-[min(72vh,700px)] min-h-[520px]">
            <WarehouseScene
              slots={visibleSlots}
              selectedSlotId={selectedSlot?.id ?? null}
              showLabels={showLabels}
              showReferenceZones={showReferenceZones}
              onSelectSlot={setSelectedSlot}
            />
          </div>
        </Panel>

        <div className="space-y-5">
          <Panel className="animate-rise">
            <h2 className="mb-4 text-base font-semibold text-slate-900">Controles</h2>
            <WarehouseControls
              selectedLevel={selectedLevel}
              showLabels={showLabels}
              showReferenceZones={showReferenceZones}
              onSelectedLevelChange={setSelectedLevel}
              onShowLabelsChange={setShowLabels}
              onShowReferenceZonesChange={setShowReferenceZones}
            />
          </Panel>

          <Panel className="animate-rise">
            <h2 className="mb-4 text-base font-semibold text-slate-900">Inspector</h2>
            <LocationInspector
              selectedSlot={selectedSlot}
              onClearSelection={() => setSelectedSlot(null)}
            />
          </Panel>

          <Panel className="animate-rise">
            <h2 className="mb-4 text-base font-semibold text-slate-900">Leyenda</h2>
            <WarehouseLegend />
          </Panel>

          <Panel className="animate-rise">
            <h2 className="mb-3 text-base font-semibold text-slate-900">Resumen</h2>
            <div className="space-y-2 text-sm text-slate-600">
              {stats.zoneSummaries.map((zone) => (
                <p key={zone.id}>
                  <span className="font-semibold text-slate-800">{zone.label}:</span>{' '}
                  {zone.faces} caras, {zone.modulesPerFace} modulos por cara.
                </p>
              ))}
            </div>
          </Panel>
        </div>
      </div>
    </div>
  )
}
