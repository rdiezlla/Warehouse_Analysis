export type RackSide = 'PAR' | 'IMPAR'

export type RackLevel = 0 | 1 | 10 | 20 | 30 | 40

export type RackBayType = 'standard-3eu' | 'split-6eu'

export type RackBayTypeOverrides = Record<string, RackBayType>

export interface WarehouseZoneConfig {
  id: string
  label: string
  startAisle: number
  endAisle: number
  startLocation: number
  endLocation: number
}

export interface RackFaceConfig {
  zoneId: string
  aisle: number
  side: RackSide
  startLocation: number
  endLocation: number
}

export interface WarehouseAisleConfig {
  aisle: number
  faces: RackFaceConfig[]
}

export interface RackFace extends RackFaceConfig {
  id: string
  x: number
  streetX: number
  rackDepthSign: number
  zStart: number
  zEnd: number
}

export interface RackLocation {
  uid: string
  id: string
  bayId: string
  faceId: string
  zoneId: string
  aisle: number
  side: RackSide
  location: number
  level: RackLevel
  bayIndex: number
  positionInsideBay: number
  x: number
  y: number
  z: number
}

export interface RackPillar {
  id: string
  faceId: string
  x: number
  z: number
}

export interface RackBeam {
  id: string
  faceId: string
  x: number
  y: number
  z: number
  length: number
}

export interface RackBay {
  id: string
  faceId: string
  zoneId: string
  aisle: number
  side: RackSide
  bayIndex: number
  startLocation: number
  endLocation: number
  x: number
  z: number
  zStart: number
  zEnd: number
  rackDepthSign: number
  length: number
}

export interface AisleMarker {
  id: string
  label: string
  aisle: number
  x: number
  z: number
  length: number
}

export interface LocationLabel {
  id: string
  label: string
  x: number
  y: number
  z: number
}

export interface ZoneSummary {
  id: string
  label: string
  aisleLabel: string
  locationLabel: string
  faceCount: number
  locationsPerLevel: number
  locationsAllLevels: number
}

export interface WarehouseReferenceZone {
  id: string
  label: string
  x: number
  z: number
  width: number
  depth: number
}
