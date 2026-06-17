import type {
  RackFaceConfig,
  RackLevel,
  RackModule,
  RackSide,
  RackSlot,
  WarehouseZoneConfig,
} from '@/features/warehouse3d/types'

export const RACK_LEVELS = [0, 10, 20, 30, 40] as const satisfies RackLevel[]
export const LOCATIONS_PER_MODULE = 6

export const MODULE_WIDTH = 1.2
export const MODULE_DEPTH = 0.45
export const LEVEL_HEIGHT = 0.65
export const FACE_GAP = 0.75
export const ZONE_GAP = 3.5

const padNumber = (value: number, size: number) => String(value).padStart(size, '0')

const face = (aisle: number, side: RackSide): RackFaceConfig => ({ aisle, side })

const buildAlternatingFaces = (
  startAisle: number,
  endAisle: number,
  firstFace: RackFaceConfig,
  lastFace: RackFaceConfig,
): RackFaceConfig[] => {
  const faces: RackFaceConfig[] = [firstFace]

  for (let aisle = startAisle + 1; aisle < endAisle; aisle += 1) {
    faces.push(face(aisle, 'IMPAR'), face(aisle, 'PAR'))
  }

  faces.push(lastFace)
  return faces
}

export const WAREHOUSE_ZONES: WarehouseZoneConfig[] = [
  {
    id: 'zone-a',
    label: 'Zona A parcial izquierda',
    startLocation: 37,
    endLocation: 120,
    faces: buildAlternatingFaces(7, 20, face(7, 'PAR'), face(20, 'IMPAR')),
  },
  {
    id: 'zone-b',
    label: 'Zona B completa derecha',
    startLocation: 1,
    endLocation: 120,
    faces: buildAlternatingFaces(20, 26, face(20, 'PAR'), face(26, 'IMPAR')),
  },
]

export const formatSlotId = ({
  aisle,
  side,
  moduleIndex,
  startLocation,
  endLocation,
  level,
}: Pick<
  RackSlot,
  'aisle' | 'side' | 'moduleIndex' | 'startLocation' | 'endLocation' | 'level'
>) =>
  `P${padNumber(aisle, 2)}-${side}-M${padNumber(moduleIndex, 2)}-U${padNumber(
    startLocation,
    3,
  )}-${padNumber(endLocation, 3)}-H${padNumber(level, 2)}`

const formatModuleId = ({
  aisle,
  side,
  moduleIndex,
  startLocation,
  endLocation,
}: Pick<
  RackModule,
  'aisle' | 'side' | 'moduleIndex' | 'startLocation' | 'endLocation'
>) =>
  `P${padNumber(aisle, 2)}-${side}-M${padNumber(moduleIndex, 2)}-U${padNumber(
    startLocation,
    3,
  )}-${padNumber(endLocation, 3)}`

export const buildRackModules = (
  zones: WarehouseZoneConfig[] = WAREHOUSE_ZONES,
): RackModule[] => {
  let zoneOffsetX = 0

  return zones.flatMap((zone) => {
    const moduleCount =
      (zone.endLocation - zone.startLocation + 1) / LOCATIONS_PER_MODULE

    const modules = zone.faces.flatMap((rackFace, faceIndex) =>
      Array.from({ length: moduleCount }, (_, moduleOffset) => {
        const moduleIndex = moduleOffset + 1
        const startLocation =
          zone.startLocation + moduleOffset * LOCATIONS_PER_MODULE
        const endLocation = startLocation + LOCATIONS_PER_MODULE - 1
        const moduleData = {
          zoneId: zone.id,
          aisle: rackFace.aisle,
          side: rackFace.side,
          moduleIndex,
          startLocation,
          endLocation,
          x: zoneOffsetX + faceIndex * FACE_GAP,
          z: moduleOffset * MODULE_WIDTH,
        }

        return {
          ...moduleData,
          id: formatModuleId(moduleData),
        }
      }),
    )

    zoneOffsetX += (zone.faces.length - 1) * FACE_GAP + ZONE_GAP
    return modules
  })
}

export const buildRackSlots = (modules: RackModule[] = buildRackModules()): RackSlot[] =>
  modules.flatMap((rackModule) =>
    RACK_LEVELS.map((level, levelIndex) => {
      const slotData = {
        ...rackModule,
        level,
        y: levelIndex * LEVEL_HEIGHT + LEVEL_HEIGHT / 2,
      }

      return {
        ...slotData,
        id: formatSlotId(slotData),
      }
    }),
  )

export const WAREHOUSE_MODULES = buildRackModules()
export const WAREHOUSE_SLOTS = buildRackSlots(WAREHOUSE_MODULES)
