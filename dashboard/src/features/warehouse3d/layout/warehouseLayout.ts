import type {
  AisleMarker,
  LocationLabel,
  RackBeam,
  RackFace,
  RackFaceConfig,
  RackLevel,
  RackLocation,
  RackPillar,
  RackSide,
  WarehouseAisleConfig,
  WarehouseZoneConfig,
  ZoneSummary,
} from '@/features/warehouse3d/types'

export const RACK_LEVELS = [0, 10, 20, 30, 40] as const satisfies RackLevel[]

export const LOCATIONS_PER_BAY = 3

export const LOCATION_WIDTH = 0.18
export const LOCATION_DEPTH = 0.34
export const LOCATION_HEIGHT = 0.42
export const LEVEL_HEIGHT = 0.58
export const RACK_DEPTH = 0.46
export const AISLE_WIDTH = 1.45
export const AISLE_SPACING = 2.75
export const BAY_GAP = 0.08
export const PILLAR_WIDTH = 0.055
export const BEAM_HEIGHT = 0.035

const FIRST_AISLE = 7
const LAST_AISLE = 26
const LOCATION_LABEL_STEP = 20

const padNumber = (value: number, size: number) => String(value).padStart(size, '0')

export const WAREHOUSE_ZONES: WarehouseZoneConfig[] = [
  {
    id: 'zone-a',
    label: 'Zona A parcial izquierda',
    startAisle: 7,
    endAisle: 20,
    startLocation: 37,
    endLocation: 120,
  },
  {
    id: 'zone-b',
    label: 'Zona B completa derecha',
    startAisle: 20,
    endAisle: 26,
    startLocation: 1,
    endLocation: 120,
  },
]

export const formatLocationId = ({
  aisle,
  side,
  location,
  level,
}: Pick<RackLocation, 'aisle' | 'side' | 'location' | 'level'>) =>
  `P${padNumber(aisle, 2)}-${side}-U${padNumber(location, 3)}-H${padNumber(level, 2)}`

const formatFaceId = ({
  aisle,
  side,
  startLocation,
  endLocation,
}: Pick<RackFaceConfig, 'aisle' | 'side' | 'startLocation' | 'endLocation'>) =>
  `P${padNumber(aisle, 2)}-${side}-U${padNumber(startLocation, 3)}-${padNumber(
    endLocation,
    3,
  )}`

const createFace = (
  zoneId: string,
  aisle: number,
  side: RackSide,
  startLocation: number,
  endLocation: number,
): RackFaceConfig => ({
  zoneId,
  aisle,
  side,
  startLocation,
  endLocation,
})

export const buildWarehouseAisles = (): WarehouseAisleConfig[] =>
  Array.from({ length: LAST_AISLE - FIRST_AISLE + 1 }, (_, index) => {
    const aisle = FIRST_AISLE + index
    const faces: RackFaceConfig[] = []

    if (aisle === 7) {
      faces.push(createFace('zone-a', aisle, 'PAR', 37, 120))
    } else if (aisle >= 8 && aisle <= 19) {
      faces.push(
        createFace('zone-a', aisle, 'IMPAR', 37, 120),
        createFace('zone-a', aisle, 'PAR', 37, 120),
      )
    } else if (aisle === 20) {
      faces.push(
        createFace('zone-a', aisle, 'IMPAR', 37, 120),
        createFace('zone-b', aisle, 'PAR', 1, 120),
      )
    } else if (aisle >= 21 && aisle <= 25) {
      faces.push(
        createFace('zone-b', aisle, 'IMPAR', 1, 120),
        createFace('zone-b', aisle, 'PAR', 1, 120),
      )
    } else if (aisle === 26) {
      faces.push(createFace('zone-b', aisle, 'IMPAR', 1, 120))
    }

    return { aisle, faces }
  })

export const getBayIndex = (startLocation: number, location: number) =>
  Math.floor((location - startLocation) / LOCATIONS_PER_BAY) + 1

export const getPositionInsideBay = (startLocation: number, location: number) =>
  ((location - startLocation) % LOCATIONS_PER_BAY) + 1

const getStreetX = (aisle: number) => (aisle - FIRST_AISLE) * AISLE_SPACING

const getRackX = (aisle: number, side: RackSide) => {
  const sideSign = side === 'IMPAR' ? -1 : 1
  return getStreetX(aisle) + sideSign * (AISLE_WIDTH / 2 + RACK_DEPTH / 2)
}

const getLocationZ = (location: number) =>
  (location - 1) * LOCATION_WIDTH + Math.floor((location - 1) / LOCATIONS_PER_BAY) * BAY_GAP

export const getLocationPosition = (
  face: RackFaceConfig,
  location: number,
  level: RackLevel,
) => {
  const levelIndex = RACK_LEVELS.indexOf(level)

  return {
    x: getRackX(face.aisle, face.side),
    y: levelIndex * LEVEL_HEIGHT + LEVEL_HEIGHT / 2,
    z: getLocationZ(location),
  }
}

export const buildRackFaces = (
  aisles: WarehouseAisleConfig[] = buildWarehouseAisles(),
): RackFace[] =>
  aisles.flatMap((aisle) =>
    aisle.faces.map((face) => ({
      ...face,
      id: formatFaceId(face),
      x: getRackX(face.aisle, face.side),
      streetX: getStreetX(face.aisle),
      zStart: getLocationZ(face.startLocation) - LOCATION_WIDTH / 2,
      zEnd: getLocationZ(face.endLocation) + LOCATION_WIDTH / 2,
    })),
  )

export const buildRackLocations = (faces: RackFace[] = buildRackFaces()): RackLocation[] =>
  faces.flatMap((face) =>
    Array.from(
      { length: face.endLocation - face.startLocation + 1 },
      (_, locationOffset) => face.startLocation + locationOffset,
    ).flatMap((location) =>
      RACK_LEVELS.map((level) => {
        const position = getLocationPosition(face, location, level)
        const locationData = {
          zoneId: face.zoneId,
          aisle: face.aisle,
          side: face.side,
          location,
          level,
          bayIndex: getBayIndex(face.startLocation, location),
          positionInsideBay: getPositionInsideBay(face.startLocation, location),
          ...position,
        }

        return {
          ...locationData,
          id: formatLocationId(locationData),
        }
      }),
    ),
  )

const getBayRanges = (face: RackFace) => {
  const ranges: Array<{ startLocation: number; endLocation: number }> = []

  for (
    let startLocation = face.startLocation;
    startLocation <= face.endLocation;
    startLocation += LOCATIONS_PER_BAY
  ) {
    ranges.push({
      startLocation,
      endLocation: Math.min(startLocation + LOCATIONS_PER_BAY - 1, face.endLocation),
    })
  }

  return ranges
}

export const buildRackPillars = (faces: RackFace[] = buildRackFaces()): RackPillar[] =>
  faces.flatMap((face) => {
    const pillars: RackPillar[] = []
    const bayRanges = getBayRanges(face)

    bayRanges.forEach((bay, index) => {
      if (index === 0) {
        pillars.push({
          id: `${face.id}-pillar-start`,
          faceId: face.id,
          x: face.x,
          z: getLocationZ(bay.startLocation) - LOCATION_WIDTH / 2 - BAY_GAP / 2,
        })
      }

      pillars.push({
        id: `${face.id}-pillar-${index + 1}`,
        faceId: face.id,
        x: face.x,
        z: getLocationZ(bay.endLocation) + LOCATION_WIDTH / 2 + BAY_GAP / 2,
      })
    })

    return pillars
  })

export const buildRackBeams = (faces: RackFace[] = buildRackFaces()): RackBeam[] =>
  faces.flatMap((face) =>
    getBayRanges(face).flatMap((bay, bayIndex) => {
      const zStart = getLocationZ(bay.startLocation) - LOCATION_WIDTH / 2
      const zEnd = getLocationZ(bay.endLocation) + LOCATION_WIDTH / 2
      const z = (zStart + zEnd) / 2
      const length = zEnd - zStart

      return Array.from({ length: RACK_LEVELS.length + 1 }, (_, levelIndex) => ({
        id: `${face.id}-bay-${bayIndex + 1}-beam-${levelIndex}`,
        faceId: face.id,
        x: face.x,
        y: levelIndex * LEVEL_HEIGHT,
        z,
        length,
      }))
    }),
  )

export const buildAisleMarkers = (
  aisles: WarehouseAisleConfig[] = buildWarehouseAisles(),
): AisleMarker[] =>
  aisles.map((aisle) => ({
    id: `aisle-${aisle.aisle}`,
    label: `P${padNumber(aisle.aisle, 2)}`,
    aisle: aisle.aisle,
    x: getStreetX(aisle.aisle),
    z: getLocationZ(60),
    length: getLocationZ(120) + LOCATION_WIDTH,
  }))

export const shouldRenderLocationLabel = (face: RackFace, location: number) =>
  location === face.startLocation ||
  location === face.endLocation ||
  (location % LOCATION_LABEL_STEP === 0 &&
    location > face.startLocation &&
    location < face.endLocation)

export const buildLocationLabels = (faces: RackFace[] = buildRackFaces()): LocationLabel[] =>
  faces.flatMap((face) =>
    Array.from(
      { length: face.endLocation - face.startLocation + 1 },
      (_, locationOffset) => face.startLocation + locationOffset,
    )
      .filter((location) => shouldRenderLocationLabel(face, location))
      .map((location) => ({
        id: `${face.id}-label-${location}`,
        label: `U${padNumber(location, 3)}`,
        x: face.x,
        y: RACK_LEVELS.length * LEVEL_HEIGHT + 0.28,
        z: getLocationZ(location),
      })),
  )

export const buildZoneSummaries = (
  faces: RackFace[] = buildRackFaces(),
): ZoneSummary[] =>
  WAREHOUSE_ZONES.map((zone) => {
    const zoneFaces = faces.filter((face) => face.zoneId === zone.id)
    const locationsPerLevel = zoneFaces.reduce(
      (total, face) => total + face.endLocation - face.startLocation + 1,
      0,
    )

    return {
      id: zone.id,
      label: zone.label,
      aisleLabel: `P${padNumber(zone.startAisle, 2)} a P${padNumber(zone.endAisle, 2)}`,
      locationLabel: `${padNumber(zone.startLocation, 3)} a ${padNumber(
        zone.endLocation,
        3,
      )}`,
      faceCount: zoneFaces.length,
      locationsPerLevel,
      locationsAllLevels: locationsPerLevel * RACK_LEVELS.length,
    }
  })

export const WAREHOUSE_AISLES = buildWarehouseAisles()
export const WAREHOUSE_FACES = buildRackFaces(WAREHOUSE_AISLES)
export const WAREHOUSE_LOCATIONS = buildRackLocations(WAREHOUSE_FACES)
export const WAREHOUSE_PILLARS = buildRackPillars(WAREHOUSE_FACES)
export const WAREHOUSE_BEAMS = buildRackBeams(WAREHOUSE_FACES)
export const WAREHOUSE_AISLE_MARKERS = buildAisleMarkers(WAREHOUSE_AISLES)
export const WAREHOUSE_LOCATION_LABELS = buildLocationLabels(WAREHOUSE_FACES)
export const WAREHOUSE_ZONE_SUMMARIES = buildZoneSummaries(WAREHOUSE_FACES)
