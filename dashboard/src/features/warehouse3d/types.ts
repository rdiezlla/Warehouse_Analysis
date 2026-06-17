export type RackSide = 'PAR' | 'IMPAR'

export type RackLevel = 0 | 10 | 20 | 30 | 40

export type RackLevelFilter = RackLevel | 'all'

export interface RackFaceConfig {
  aisle: number
  side: RackSide
}

export interface WarehouseZoneConfig {
  id: string
  label: string
  startLocation: number
  endLocation: number
  faces: RackFaceConfig[]
}

export interface RackModule {
  id: string
  zoneId: string
  aisle: number
  side: RackSide
  moduleIndex: number
  startLocation: number
  endLocation: number
  x: number
  z: number
}

export interface RackSlot {
  id: string
  zoneId: string
  aisle: number
  side: RackSide
  moduleIndex: number
  startLocation: number
  endLocation: number
  level: RackLevel
  x: number
  y: number
  z: number
}
