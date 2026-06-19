import { useMemo, useState } from 'react'

import { Panel } from '@/components/ui/Panel'
import { LocationInspector } from '@/features/warehouse3d/components/LocationInspector'
import { WarehouseControls } from '@/features/warehouse3d/components/WarehouseControls'
import { WarehouseLegend } from '@/features/warehouse3d/components/WarehouseLegend'
import { WarehouseScene } from '@/features/warehouse3d/components/WarehouseScene'
import { useRackBayTypeOverrides } from '@/features/warehouse3d/hooks/useRackBayTypeOverrides'
import {
  buildRenderableRackSlots,
  getBayType,
  WAREHOUSE_BAYS,
  WAREHOUSE_LOCATIONS,
  WAREHOUSE_ZONE_SUMMARIES,
} from '@/features/warehouse3d/layout/warehouseLayout'
import type { RackBayType, RackLocation } from '@/features/warehouse3d/types'

export const Warehouse3DModule = () => {
  const [showLabels, setShowLabels] = useState(true)
  const [showReferenceZones, setShowReferenceZones] = useState(true)
  const [editRackTypes, setEditRackTypes] = useState(false)
  const [rackTypeToApply, setRackTypeToApply] =
    useState<RackBayType>('split-6eu')
  const [selectedLocation, setSelectedLocation] = useState<RackLocation | null>(null)
  const { overrides, setBayType, resetBayTypes } = useRackBayTypeOverrides()

  const renderableLocations = useMemo(
    () => buildRenderableRackSlots(WAREHOUSE_LOCATIONS, overrides),
    [overrides],
  )

  const stats = useMemo(() => {
    const splitBays = WAREHOUSE_BAYS.filter(
      (bay) => getBayType(bay.id, overrides) === 'split-6eu',
    )
    const zoneSummaries = WAREHOUSE_ZONE_SUMMARIES.map((zone) => {
      const splitBayCount = splitBays.filter((bay) => bay.zoneId === zone.id).length

      return {
        ...zone,
        selectableLocations: zone.locationsPerLevel + splitBayCount * 3,
      }
    })

    return {
      selectableLocations: renderableLocations.length,
      standardBayCount: WAREHOUSE_BAYS.length - splitBays.length,
      splitBayCount: splitBays.length,
      zoneSummaries,
    }
  }, [overrides, renderableLocations.length])

  const selectedRackType = selectedLocation
    ? getBayType(selectedLocation.bayId, overrides)
    : null

  const handleEditRackTypesChange = (enabled: boolean) => {
    setEditRackTypes(enabled)

    if (enabled) {
      setSelectedLocation(null)
    }
  }

  const handleEditRackBay = (bayId: string) => {
    setBayType(bayId, rackTypeToApply)
    setSelectedLocation(null)
  }

  const handleResetRackTypes = () => {
    resetBayTypes()
    setSelectedLocation(null)
  }

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
              {stats.selectableLocations} ubicaciones seleccionables
            </div>
          </div>

          <div className="h-[min(72vh,700px)] min-h-[520px]">
            <WarehouseScene
              locations={renderableLocations}
              selectedLocation={selectedLocation}
              showLabels={showLabels}
              showReferenceZones={showReferenceZones}
              rackBayTypeOverrides={overrides}
              editRackTypes={editRackTypes}
              onSelectLocation={setSelectedLocation}
              onEditRackBay={handleEditRackBay}
            />
          </div>
        </Panel>

        <div className="space-y-5">
          <Panel className="animate-rise">
            <h2 className="mb-4 text-base font-semibold text-slate-900">Controles</h2>
            <WarehouseControls
              showLabels={showLabels}
              showReferenceZones={showReferenceZones}
              editRackTypes={editRackTypes}
              selectedRackType={rackTypeToApply}
              hasRackTypeOverrides={stats.splitBayCount > 0}
              onShowLabelsChange={setShowLabels}
              onShowReferenceZonesChange={setShowReferenceZones}
              onEditRackTypesChange={handleEditRackTypesChange}
              onSelectedRackTypeChange={setRackTypeToApply}
              onResetRackTypes={handleResetRackTypes}
            />
          </Panel>

          <Panel className="animate-rise">
            <h2 className="mb-4 text-base font-semibold text-slate-900">Inspector</h2>
            <LocationInspector
              selectedLocation={selectedLocation}
              selectedRackType={selectedRackType}
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
                  {zone.faceCount} caras, {zone.selectableLocations} ubicaciones seleccionables.
                </p>
              ))}
              <p>
                <span className="font-semibold text-slate-800">Bays estándar:</span>{' '}
                {stats.standardBayCount}
              </p>
              <p>
                <span className="font-semibold text-slate-800">Bays Rack 6 EU:</span>{' '}
                {stats.splitBayCount}
              </p>
              <p>
                <span className="font-semibold text-slate-800">Total:</span>{' '}
                {stats.selectableLocations} ubicaciones seleccionables.
              </p>
            </div>
          </Panel>
        </div>
      </div>
    </div>
  )
}
