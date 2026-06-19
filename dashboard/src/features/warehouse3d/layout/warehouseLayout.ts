import type {
  AisleMarker,
  LocationLabel,
  RackBay,
  RackBayType,
  RackBayTypeOverrides,
  RackFace,
  RackFaceConfig,
  RackLevel,
  RackLocation,
  RackSide,
  WarehouseAisleConfig,
  WarehouseReferenceZone,
  WarehouseZoneConfig,
  ZoneSummary,
} from '@/features/warehouse3d/types'

export const RACK_LEVELS = [0, 10, 20, 30, 40] as const satisfies RackLevel[]
export const VISIBLE_RACK_LEVELS: readonly RackLevel[] = [0]

export const LOCATIONS_PER_BAY = 3
export const EXCLUDED_LOCATION_RANGES = [
  { start: 55, end: 60 },
  { start: 115, end: 120 },
] as const

export const isLocationExcluded = (location: number) =>
  EXCLUDED_LOCATION_RANGES.some(
    ({ start, end }) => location >= start && location <= end,
  )

export const LOCATION_WIDTH = 0.85
export const LOCATION_DEPTH = 0.95
export const LOCATION_HEIGHT = 1.2
export const LOCATION_GAP = 0.04
export const BAY_WIDTH = LOCATIONS_PER_BAY * LOCATION_WIDTH + (LOCATIONS_PER_BAY - 1) * LOCATION_GAP
export const BAY_GAP = 0.22
export const POST_WIDTH = 0.12
export const POST_HEIGHT = 1.75
export const BEAM_HEIGHT = 0.12
export const BEAM_DEPTH = 0.14
export const SPLIT_LOCATION_HEIGHT = (LOCATION_HEIGHT - BEAM_HEIGHT) / 2
export const SPLIT_BOTTOM_LOCATION_Y = SPLIT_LOCATION_HEIGHT / 2
export const SPLIT_TOP_LOCATION_Y = LOCATION_HEIGHT - SPLIT_LOCATION_HEIGHT / 2
export const SPLIT_MIDDLE_BEAM_Y = LOCATION_HEIGHT / 2
export const RACK_DEPTH = LOCATION_DEPTH + POST_WIDTH * 2
export const RACK_VISUAL_HEIGHT = POST_HEIGHT
export const AISLE_WIDTH = 2.4
export const AISLE_SPACING = 4.2

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
  location,
  level,
}: Pick<RackLocation, 'aisle' | 'location' | 'level'>) =>
  `${padNumber(aisle, 2)}-${padNumber(location, 3)}-${padNumber(level, 2)}`

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

export const getLocationPairIndex = (location: number) => Math.floor((location - 1) / 2)

export const getFaceLocations = (
  face: Pick<RackFaceConfig, 'side' | 'startLocation' | 'endLocation'>,
) =>
  Array.from(
    { length: face.endLocation - face.startLocation + 1 },
    (_, locationOffset) => face.startLocation + locationOffset,
  ).filter((location) => {
    const matchesSide = face.side === 'PAR' ? location % 2 === 0 : location % 2 !== 0

    return matchesSide && !isLocationExcluded(location)
  })

const getFirstPhysicalFaceLocation = (
  face: Pick<RackFaceConfig, 'side' | 'startLocation' | 'endLocation'>,
) => {
  const startMatchesSide =
    face.side === 'PAR'
      ? face.startLocation % 2 === 0
      : face.startLocation % 2 !== 0

  return startMatchesSide ? face.startLocation : face.startLocation + 1
}

const getPhysicalFaceLocationIndex = (
  face: Pick<RackFaceConfig, 'side' | 'startLocation' | 'endLocation'>,
  location: number,
) => {
  const matchesSide = face.side === 'PAR' ? location % 2 === 0 : location % 2 !== 0

  if (
    !matchesSide ||
    isLocationExcluded(location) ||
    location < face.startLocation ||
    location > face.endLocation
  ) {
    return -1
  }

  return (
    getLocationPairIndex(location) -
    getLocationPairIndex(getFirstPhysicalFaceLocation(face))
  )
}

export const getBayIndexForFaceLocation = (
  face: Pick<RackFaceConfig, 'side' | 'startLocation' | 'endLocation'>,
  location: number,
) => {
  const faceLocationIndex = getPhysicalFaceLocationIndex(face, location)

  return faceLocationIndex < 0
    ? -1
    : Math.floor(faceLocationIndex / LOCATIONS_PER_BAY) + 1
}

export const getPositionInsideBayForFaceLocation = (
  face: Pick<RackFaceConfig, 'side' | 'startLocation' | 'endLocation'>,
  location: number,
) => {
  const faceLocationIndex = getPhysicalFaceLocationIndex(face, location)

  return faceLocationIndex < 0
    ? -1
    : (faceLocationIndex % LOCATIONS_PER_BAY) + 1
}

const getStreetX = (aisle: number) => (aisle - FIRST_AISLE) * AISLE_SPACING

export const getRackDepthSign = (side: RackSide) => (side === 'IMPAR' ? 1 : -1)

export const getRackX = (aisle: number, side: RackSide) => {
  const sideSign = side === 'IMPAR' ? -1 : 1
  return getStreetX(aisle) + sideSign * (AISLE_WIDTH / 2 + RACK_DEPTH / 2)
}

export const getLocationZ = (location: number) => {
  const pairIndex = getLocationPairIndex(location)
  const bayIndex = Math.floor(pairIndex / LOCATIONS_PER_BAY)
  const positionInsideBay = pairIndex % LOCATIONS_PER_BAY

  return bayIndex * (BAY_WIDTH + BAY_GAP) + positionInsideBay * (LOCATION_WIDTH + LOCATION_GAP)
}

export const getLocationPosition = (
  face: RackFaceConfig,
  location: number,
  level: RackLevel,
) => {
  const levelIndex = VISIBLE_RACK_LEVELS.indexOf(level)

  return {
    x: getRackX(face.aisle, face.side),
    y: levelIndex * LOCATION_HEIGHT + LOCATION_HEIGHT / 2,
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
      rackDepthSign: getRackDepthSign(face.side),
      zStart: getLocationZ(face.startLocation) - LOCATION_WIDTH / 2,
      zEnd: getLocationZ(face.endLocation) + LOCATION_WIDTH / 2,
    })),
  )

const formatBayId = (face: RackFace, bayIndex: number) => `${face.id}-bay-${bayIndex}`

export const buildRackLocations = (faces: RackFace[] = buildRackFaces()): RackLocation[] =>
  faces.flatMap((face) =>
    getFaceLocations(face).flatMap((location) =>
      VISIBLE_RACK_LEVELS.map((level) => {
        const bayIndex = getBayIndexForFaceLocation(face, location)
        const positionInsideBay = getPositionInsideBayForFaceLocation(face, location)
        const position = getLocationPosition(face, location, level)
        const locationData = {
          bayId: formatBayId(face, bayIndex),
          faceId: face.id,
          zoneId: face.zoneId,
          aisle: face.aisle,
          side: face.side,
          location,
          level,
          bayIndex,
          positionInsideBay,
          ...position,
        }

        const id = formatLocationId(locationData)

        return {
          ...locationData,
          id,
          uid: `${face.id}-${id}`,
        }
      }),
    ),
  )

export const getBayType = (
  bayId: string,
  overrides: RackBayTypeOverrides,
): RackBayType => overrides[bayId] ?? 'standard-3eu'

export const buildRenderableRackSlots = (
  baseLocations: RackLocation[],
  overrides: RackBayTypeOverrides,
): RackLocation[] =>
  baseLocations.flatMap((location) => {
    if (getBayType(location.bayId, overrides) === 'standard-3eu') {
      return location
    }

    const bottomLocation: RackLocation = {
      ...location,
      y: SPLIT_BOTTOM_LOCATION_Y,
    }
    const topLocationData: RackLocation = {
      ...location,
      level: 1,
      y: SPLIT_TOP_LOCATION_Y,
      id: formatLocationId({
        aisle: location.aisle,
        location: location.location,
        level: 1,
      }),
      uid: '',
    }

    topLocationData.uid = `${topLocationData.faceId}-${topLocationData.id}`

    return [bottomLocation, topLocationData]
  })

const getBayRanges = (face: RackFace) => {
  const locationsByBay = new Map<number, number[]>()

  getFaceLocations(face).forEach((location) => {
    const bayIndex = getBayIndexForFaceLocation(face, location)
    const bayLocations = locationsByBay.get(bayIndex) ?? []
    bayLocations.push(location)
    locationsByBay.set(bayIndex, bayLocations)
  })

  return Array.from(locationsByBay.entries()).map(([bayIndex, locations]) => ({
    bayIndex,
    startLocation: locations[0],
    endLocation: locations[locations.length - 1],
  }))
}

export const buildRackBays = (faces: RackFace[] = buildRackFaces()): RackBay[] =>
  faces.flatMap((face) =>
    getBayRanges(face).map((bay) => {
      const bayIndex = bay.bayIndex
      const zStart = getLocationZ(bay.startLocation) - LOCATION_WIDTH / 2
      const zEnd = getLocationZ(bay.endLocation) + LOCATION_WIDTH / 2

      return {
        id: formatBayId(face, bayIndex),
        faceId: face.id,
        zoneId: face.zoneId,
        aisle: face.aisle,
        side: face.side,
        bayIndex,
        startLocation: bay.startLocation,
        endLocation: bay.endLocation,
        x: face.x,
        z: (zStart + zEnd) / 2,
        zStart,
        zEnd,
        rackDepthSign: face.rackDepthSign,
        length: zEnd - zStart,
      }
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
    z: (getLocationZ(1) + getLocationZ(120)) / 2,
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
    getFaceLocations(face)
      .filter((location) => shouldRenderLocationLabel(face, location))
      .map((location) => ({
        id: `${face.id}-label-${location}`,
        label: `U${padNumber(location, 3)}`,
        x: face.x,
        y: RACK_VISUAL_HEIGHT + 0.28,
        z: getLocationZ(location),
      })),
  )

export const buildReferenceZones = (faces: RackFace[] = buildRackFaces()): WarehouseReferenceZone[] => {
  const zoneAFaces = faces.filter((face) => face.zoneId === 'zone-a')
  const minX = Math.min(...zoneAFaces.map((face) => face.x)) - RACK_DEPTH
  const maxX = Math.max(...zoneAFaces.map((face) => face.x)) + RACK_DEPTH
  const width = maxX - minX
  const startZ = getLocationZ(1) - LOCATION_WIDTH / 2
  const endZ = getLocationZ(36) + LOCATION_WIDTH / 2
  const depth = endZ - startZ
  const z = (startZ + endZ) / 2

  return [
    {
      id: 'playas-mahou',
      label: 'Playas recepcion/expedicion Mahou',
      x: minX + width * 0.25,
      z,
      width: width * 0.43,
      depth,
    },
    {
      id: 'playas-red-bull',
      label: 'Playas recepcion/expedicion Red Bull',
      x: minX + width * 0.67,
      z,
      width: width * 0.31,
      depth,
    },
    {
      id: 'cross-docking',
      label: 'Zona cross-docking',
      x: minX + width * 0.91,
      z,
      width: width * 0.15,
      depth,
    },
    {
      id: 'suelo-1',
      label: 'Almacenamiento suelo 1',
      x: minX - 3.8,
      z: getLocationZ(18),
      width: 3,
      depth: getLocationZ(36),
    },
    {
      id: 'suelo-2',
      label: 'Almacenamiento suelo 2',
      x: minX - 0.6,
      z: getLocationZ(18),
      width: 3,
      depth: getLocationZ(36),
    },
  ]
}

export const buildZoneSummaries = (
  faces: RackFace[] = buildRackFaces(),
): ZoneSummary[] =>
  WAREHOUSE_ZONES.map((zone) => {
    const zoneFaces = faces.filter((face) => face.zoneId === zone.id)
    const locationsPerLevel = zoneFaces.reduce(
      (total, face) => total + getFaceLocations(face).length,
      0,
    )

    return {
      id: zone.id,
      label: zone.label,
      aisleLabel: `P${padNumber(zone.startAisle, 2)} a P${padNumber(zone.endAisle, 2)}`,
      locationLabel: `U${padNumber(zone.startLocation, 3)} a U${padNumber(
        zone.endLocation,
        3,
      )}`,
      faceCount: zoneFaces.length,
      locationsPerLevel,
      locationsAllLevels: locationsPerLevel,
    }
  })

export const WAREHOUSE_AISLES = buildWarehouseAisles()
export const WAREHOUSE_FACES = buildRackFaces(WAREHOUSE_AISLES)
export const WAREHOUSE_BAYS = buildRackBays(WAREHOUSE_FACES)
export const WAREHOUSE_LOCATIONS = buildRackLocations(WAREHOUSE_FACES)
export const WAREHOUSE_AISLE_MARKERS = buildAisleMarkers(WAREHOUSE_AISLES)
export const WAREHOUSE_LOCATION_LABELS = buildLocationLabels(WAREHOUSE_FACES)
export const WAREHOUSE_REFERENCE_ZONES = buildReferenceZones(WAREHOUSE_FACES)
export const WAREHOUSE_ZONE_SUMMARIES = buildZoneSummaries(WAREHOUSE_FACES)

export const validateWarehouseLayout = () => {
  const errors: string[] = []
  const p07 = WAREHOUSE_AISLE_MARKERS.find((aisle) => aisle.aisle === 7)
  const p26 = WAREHOUSE_AISLE_MARKERS.find((aisle) => aisle.aisle === 26)
  const zoneAFaces = WAREHOUSE_FACES.filter((face) => face.zoneId === 'zone-a')
  const zoneBFaces = WAREHOUSE_FACES.filter((face) => face.zoneId === 'zone-b')
  const hasLocation = (aisle: number, side: RackSide, locationNumber: number) =>
    WAREHOUSE_LOCATIONS.some(
      (location) =>
        location.aisle === aisle &&
        location.side === side &&
        location.location === locationNumber &&
        location.level === 0,
    )
  const referenceZoneIds = new Set(['playas-mahou', 'playas-red-bull', 'cross-docking'])
  const beachZones = WAREHOUSE_REFERENCE_ZONES.filter((zone) => referenceZoneIds.has(zone.id))
  const zoneBMinX = Math.min(...zoneBFaces.map((face) => face.x))
  const beachMaxX = Math.max(
    ...beachZones.map((zone) => zone.x + zone.width / 2),
  )
  const beachMinZ = Math.min(
    ...beachZones.map((zone) => zone.z - zone.depth / 2),
  )
  const beachMaxZ = Math.max(
    ...beachZones.map((zone) => zone.z + zone.depth / 2),
  )

  if (!p07 || !p26 || p07.x >= p26.x) {
    errors.push('Layout invalido: P07 debe quedar a la izquierda de P26.')
  }

  if (getLocationZ(1) >= getLocationZ(120)) {
    errors.push('Layout invalido: U001 debe tener menor Z que U120.')
  }

  if (getLocationZ(1) !== getLocationZ(2)) {
    errors.push('Layout invalido: U001 y U002 deben compartir posicion Z.')
  }

  if (getLocationZ(37) !== getLocationZ(38)) {
    errors.push('Layout invalido: U037 y U038 deben compartir posicion Z.')
  }

  if (getLocationZ(119) !== getLocationZ(120)) {
    errors.push('Layout invalido: U119 y U120 deben compartir posicion Z.')
  }

  if (zoneAFaces.some((face) => face.startLocation !== 37)) {
    errors.push('Layout invalido: Zona A debe empezar en U037.')
  }

  if (
    zoneAFaces.some(
      (face) =>
        getFaceLocations(face)[0] !== (face.side === 'IMPAR' ? 37 : 38),
    )
  ) {
    errors.push('Layout invalido: Zona A debe empezar en U037/U038 segun el lado.')
  }

  if (zoneBFaces.some((face) => face.startLocation !== 1)) {
    errors.push('Layout invalido: Zona B debe empezar en U001.')
  }

  if (
    zoneBFaces.some(
      (face) => getFaceLocations(face)[0] !== (face.side === 'IMPAR' ? 1 : 2),
    )
  ) {
    errors.push('Layout invalido: Zona B debe empezar en U001/U002 segun el lado.')
  }

  if (beachMinZ < getLocationZ(1) - LOCATION_WIDTH || beachMaxZ > getLocationZ(36) + LOCATION_WIDTH) {
    errors.push('Layout invalido: las playas deben estar en el rango U001-U036.')
  }

  if (beachMaxX >= zoneBMinX) {
    errors.push('Layout invalido: las playas deben quedar a la izquierda, sin invadir Zona B.')
  }

  if (hasLocation(7, 'PAR', 37)) {
    errors.push('Layout invalido: 07-037-00 no debe existir en el lado PAR.')
  }

  if (!hasLocation(7, 'PAR', 38)) {
    errors.push('Layout invalido: debe existir 07-038-00 en el lado PAR.')
  }

  if (!hasLocation(8, 'IMPAR', 37)) {
    errors.push('Layout invalido: debe existir 08-037-00 en el lado IMPAR.')
  }

  if (!hasLocation(8, 'PAR', 38)) {
    errors.push('Layout invalido: debe existir 08-038-00 en el lado PAR.')
  }

  if (hasLocation(8, 'IMPAR', 38)) {
    errors.push('Layout invalido: 08-038-00 no debe existir en el lado IMPAR.')
  }

  if (hasLocation(8, 'PAR', 37)) {
    errors.push('Layout invalido: 08-037-00 no debe existir en el lado PAR.')
  }

  if (!hasLocation(20, 'PAR', 2) || hasLocation(20, 'PAR', 1)) {
    errors.push('Layout invalido: P20 PAR debe empezar en 20-002-00.')
  }

  if (WAREHOUSE_LOCATIONS.some(({ location }) => isLocationExcluded(location))) {
    errors.push('Layout invalido: existen ubicaciones dentro de los rangos excluidos.')
  }

  if (
    WAREHOUSE_LOCATIONS.some(({ location, side }) =>
      side === 'PAR' ? location % 2 !== 0 : location % 2 === 0,
    )
  ) {
    errors.push('Layout invalido: la paridad de alguna ubicacion no coincide con su lado.')
  }

  const requiredLocations: Array<[number, RackSide, number]> = [
    [8, 'IMPAR', 53],
    [8, 'PAR', 54],
    [8, 'IMPAR', 61],
    [8, 'PAR', 62],
    [8, 'IMPAR', 113],
    [8, 'PAR', 114],
  ]
  const forbiddenLocations: Array<[number, RackSide, number]> = [
    [8, 'IMPAR', 55],
    [8, 'PAR', 56],
    [8, 'IMPAR', 115],
    [8, 'PAR', 116],
  ]

  if (requiredLocations.some(([aisle, side, location]) => !hasLocation(aisle, side, location))) {
    errors.push('Layout invalido: faltan ubicaciones limite junto a los huecos transversales.')
  }

  if (forbiddenLocations.some(([aisle, side, location]) => hasLocation(aisle, side, location))) {
    errors.push('Layout invalido: existen ubicaciones dentro de un pasillo transversal.')
  }

  const normalLocationStep = getLocationZ(53) - getLocationZ(51)
  const transverseGap = getLocationZ(61) - getLocationZ(53)

  if (getLocationZ(53) >= getLocationZ(61) || transverseGap <= normalLocationStep * 3) {
    errors.push('Layout invalido: no se conserva el hueco fisico entre U053/U054 y U061/U062.')
  }

  if (
    WAREHOUSE_BAYS.some(
      (bay) => isLocationExcluded(bay.startLocation) || isLocationExcluded(bay.endLocation),
    )
  ) {
    errors.push('Layout invalido: se ha generado estructura de rack en un vano excluido.')
  }

  const zoneACount = WAREHOUSE_LOCATIONS.filter(
    (location) => location.zoneId === 'zone-a',
  ).length
  const zoneBCount = WAREHOUSE_LOCATIONS.filter(
    (location) => location.zoneId === 'zone-b',
  ).length

  if (zoneACount !== 936 || zoneBCount !== 648 || WAREHOUSE_LOCATIONS.length !== 1584) {
    errors.push('Layout invalido: los totales H00 deben ser 936 + 648 = 1584.')
  }

  if (WAREHOUSE_BAYS.some((bay) => getBayType(bay.id, {}) !== 'standard-3eu')) {
    errors.push('Layout invalido: todos los vanos deben ser Rack 3 EU por defecto.')
  }

  const sampleBay = WAREHOUSE_BAYS.find(
    (bay) => bay.aisle === 8 && bay.side === 'PAR' && bay.startLocation === 38,
  )

  if (!sampleBay) {
    errors.push('Layout invalido: falta el vano de validacion P08 PAR U038-U042.')
  } else {
    const sampleLocations = WAREHOUSE_LOCATIONS.filter(
      (location) => location.bayId === sampleBay.id,
    )
    const standardSlots = buildRenderableRackSlots(sampleLocations, {})
    const splitSlots = buildRenderableRackSlots(sampleLocations, {
      [sampleBay.id]: 'split-6eu',
    })
    const lowerIds = splitSlots.filter(({ level }) => level === 0).map(({ id }) => id)
    const upperIds = splitSlots.filter(({ level }) => level === 1).map(({ id }) => id)

    if (standardSlots.length !== 3 || standardSlots.some(({ level }) => level !== 0)) {
      errors.push('Layout invalido: un Rack 3 EU debe generar tres ubicaciones nivel 00.')
    }

    if (
      splitSlots.length !== 6 ||
      lowerIds.join(',') !== '08-038-00,08-040-00,08-042-00' ||
      upperIds.join(',') !== '08-038-01,08-040-01,08-042-01'
    ) {
      errors.push('Layout invalido: un Rack 6 EU debe duplicar las tres ubicaciones en nivel 01.')
    }
  }

  errors.forEach((error) => console.error(error))

  return errors
}

validateWarehouseLayout()
