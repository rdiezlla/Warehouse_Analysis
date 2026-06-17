import { useEffect, useMemo } from 'react'
import { Canvas, type ThreeEvent, useThree } from '@react-three/fiber'
import { Edges, OrbitControls, Text } from '@react-three/drei'

import type { RackSlot } from '@/features/warehouse3d/types'
import {
  LEVEL_HEIGHT,
  MODULE_DEPTH,
  MODULE_WIDTH,
  WAREHOUSE_MODULES,
  WAREHOUSE_ZONES,
} from '@/features/warehouse3d/layout/warehouseLayout'

interface WarehouseSceneProps {
  slots: RackSlot[]
  selectedSlotId: string | null
  showLabels: boolean
  showReferenceZones: boolean
  onSelectSlot: (slot: RackSlot) => void
}

interface FaceLabel {
  id: string
  label: string
  x: number
  z: number
  zoneId: string
}

const zoneColors: Record<string, string> = {
  'zone-a': '#0891b2',
  'zone-b': '#059669',
}

const referenceZones = [
  { label: 'Almacenamiento suelo 1', x: -4.8, z: 7.5, width: 3.2, depth: 8.8 },
  { label: 'Almacenamiento suelo 2', x: -1.2, z: 7.5, width: 3.2, depth: 8.8 },
  {
    label: 'Playas recepcion/expedicion Mahou',
    x: 6.5,
    z: 22,
    width: 10,
    depth: 4.5,
  },
  {
    label: 'Playas recepcion/expedicion Red Bull',
    x: 18.4,
    z: 22,
    width: 8,
    depth: 4.5,
  },
  { label: 'Zona cross-docking', x: 26.2, z: 22, width: 4.2, depth: 4.5 },
]

const getBounds = (items: Array<Pick<RackSlot, 'x' | 'z'>>) => {
  const xs = items.map((item) => item.x)
  const zs = items.map((item) => item.z)

  return {
    minX: Math.min(...xs, -6),
    maxX: Math.max(...xs, 30),
    minZ: Math.min(...zs, 0),
    maxZ: Math.max(...zs, 25),
  }
}

const buildFaceLabels = (): FaceLabel[] =>
  WAREHOUSE_ZONES.flatMap((zone) =>
    zone.faces.map((faceConfig) => {
      const faceModules = WAREHOUSE_MODULES.filter(
        (rackModule) =>
          rackModule.zoneId === zone.id &&
          rackModule.aisle === faceConfig.aisle &&
          rackModule.side === faceConfig.side,
      )
      const firstModule = faceModules[0]
      const lastModule = faceModules.at(-1)

      return {
        id: `${zone.id}-${faceConfig.aisle}-${faceConfig.side}`,
        label: `P${String(faceConfig.aisle).padStart(2, '0')} ${faceConfig.side}`,
        x: firstModule?.x ?? 0,
        z: lastModule ? lastModule.z + MODULE_WIDTH * 0.75 : 0,
        zoneId: zone.id,
      }
    }),
  )

const RackSlotBox = ({
  slot,
  isSelected,
  onSelectSlot,
}: {
  slot: RackSlot
  isSelected: boolean
  onSelectSlot: (slot: RackSlot) => void
}) => {
  const color = isSelected ? '#f59e0b' : zoneColors[slot.zoneId] ?? '#64748b'

  const handleClick = (event: ThreeEvent<MouseEvent>) => {
    event.stopPropagation()
    onSelectSlot(slot)
  }

  return (
    <mesh position={[slot.x, slot.y, slot.z]} onClick={handleClick}>
      <boxGeometry args={[MODULE_DEPTH, LEVEL_HEIGHT * 0.72, MODULE_WIDTH * 0.82]} />
      <meshStandardMaterial color={color} transparent opacity={isSelected ? 0.95 : 0.34} />
      <Edges color={isSelected ? '#78350f' : '#334155'} />
    </mesh>
  )
}

const ReferenceZonePlates = () => (
  <group>
    {referenceZones.map((zone) => (
      <group key={zone.label} position={[zone.x, 0.015, zone.z]}>
        <mesh rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[zone.width, zone.depth]} />
          <meshStandardMaterial color="#cbd5e1" transparent opacity={0.36} />
        </mesh>
        <Text
          position={[0, 0.04, 0]}
          rotation={[-Math.PI / 2, 0, 0]}
          fontSize={0.22}
          maxWidth={zone.width - 0.4}
          textAlign="center"
          color="#475569"
          anchorX="center"
          anchorY="middle"
        >
          {zone.label}
        </Text>
      </group>
    ))}
  </group>
)

const CameraSetup = () => {
  const { camera } = useThree()

  useEffect(() => {
    camera.position.set(18, 15, 28)
    camera.lookAt(0, 1.4, 0)
    camera.updateProjectionMatrix()
  }, [camera])

  return null
}

export const WarehouseScene = ({
  slots,
  selectedSlotId,
  showLabels,
  showReferenceZones,
  onSelectSlot,
}: WarehouseSceneProps) => {
  const bounds = useMemo(() => getBounds(WAREHOUSE_MODULES), [])
  const faceLabels = useMemo(() => buildFaceLabels(), [])
  const centerX = (bounds.minX + bounds.maxX) / 2
  const centerZ = (bounds.minZ + bounds.maxZ) / 2
  const floorWidth = bounds.maxX - bounds.minX + 10
  const floorDepth = bounds.maxZ - bounds.minZ + 9

  return (
    <Canvas
      camera={{ position: [18, 15, 28], fov: 42, near: 0.1, far: 1000 }}
      gl={{ antialias: true, preserveDrawingBuffer: true }}
      onCreated={({ camera }) => camera.lookAt(0, 1.4, 0)}
      className="h-full w-full"
    >
      <color attach="background" args={['#f8fafc']} />
      <ambientLight intensity={0.72} />
      <directionalLight position={[8, 14, 10]} intensity={1.15} />
      <group position={[-centerX, 0, -centerZ]}>
        <mesh position={[centerX, -0.02, centerZ + 2]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[floorWidth, floorDepth]} />
          <meshStandardMaterial color="#e2e8f0" />
        </mesh>

        {showReferenceZones && <ReferenceZonePlates />}

        {slots.map((slot) => (
          <RackSlotBox
            key={slot.id}
            slot={slot}
            isSelected={slot.id === selectedSlotId}
            onSelectSlot={onSelectSlot}
          />
        ))}

        {showLabels &&
          faceLabels.map((faceLabel) => (
            <Text
              key={faceLabel.id}
              position={[faceLabel.x, 3.75, faceLabel.z]}
              rotation={[-Math.PI / 4, 0, 0]}
              fontSize={0.25}
              color={zoneColors[faceLabel.zoneId] ?? '#0f172a'}
              anchorX="center"
              anchorY="middle"
            >
              {faceLabel.label}
            </Text>
          ))}
      </group>
      <OrbitControls makeDefault target={[0, 1.4, 0]} maxPolarAngle={Math.PI / 2.08} />
      <CameraSetup />
    </Canvas>
  )
}
