import { useMemo, useState } from 'react'

import { Panel } from '@/components/ui/Panel'
import { LocationInspector } from '@/features/warehouse3d/components/LocationInspector'
import { WarehouseControls } from '@/features/warehouse3d/components/WarehouseControls'
import { WarehouseLegend } from '@/features/warehouse3d/components/WarehouseLegend'
import { WarehouseScene } from '@/features/warehouse3d/components/WarehouseScene'
import {
  WAREHOUSE_LOCATIONS,
  WAREHOUSE_ZONE_SUMMARIES,
} from '@/features/warehouse3d/layout/warehouseLayout'
import type { RackLocation } from '@/features/warehouse3d/types'

export const Warehouse3DModule = () => {
  const [showLabels, setShowLabels] = useState(true)
  const [showReferenceZones, setShowReferenceZones] = useState(true)
  const [selectedLocation, setSelectedLocation] = useState<RackLocation | null>(null)

  const stats = useMemo(() => {
    const selectableLocations = WAREHOUSE_ZONE_SUMMARIES.reduce(
      (total, zone) => total + zone.locationsPerLevel,
      0,
    )

    return {
      selectableLocations,
      zoneSummaries: WAREHOUSE_ZONE_SUMMARIES,
    }
  }, [])

  const getOperationalRange = (zoneId: string) =>
    zoneId === 'zone-a' ? 'P07 PAR a P20 IMPAR' : 'P20 PAR a P26 IMPAR'

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
          Representacion inicial de racks vacios en altura 0 por pasillo, vano y ubicacion EU.
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
                Pasillos P07-P26 con calles, bays de 3 ubicaciones, pilares azules y largueros rojos.
              </p>
            </div>
            <div className="text-right text-xs font-medium text-slate-500">
              {stats.selectableLocations} ubicaciones seleccionables en H00
            </div>
          </div>

          <div className="h-[min(72vh,700px)] min-h-[520px]">
            <WarehouseScene
              locations={WAREHOUSE_LOCATIONS}
              selectedLocation={selectedLocation}
              showLabels={showLabels}
              showReferenceZones={showReferenceZones}
              onSelectLocation={setSelectedLocation}
            />
          </div>
        </Panel>

        <div className="space-y-5">
          <Panel className="animate-rise">
            <h2 className="mb-4 text-base font-semibold text-slate-900">Controles</h2>
            <WarehouseControls
              showLabels={showLabels}
              showReferenceZones={showReferenceZones}
              onShowLabelsChange={setShowLabels}
              onShowReferenceZonesChange={setShowReferenceZones}
            />
          </Panel>

          <Panel className="animate-rise">
            <h2 className="mb-4 text-base font-semibold text-slate-900">Inspector</h2>
            <LocationInspector
              selectedLocation={selectedLocation}
              onClearSelection={() => setSelectedLocation(null)}
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
                  {getOperationalRange(zone.id)}, ubicaciones {zone.locationLabel},{' '}
                  {zone.faceCount} caras, {zone.locationsPerLevel} ubicaciones seleccionables en H00.
                </p>
              ))}
              <p>
                <span className="font-semibold text-slate-800">Total:</span>{' '}
                {stats.selectableLocations} ubicaciones seleccionables en altura 0.
              </p>
            </div>
          </Panel>
        </div>
      </div>
    </div>
  )
}
