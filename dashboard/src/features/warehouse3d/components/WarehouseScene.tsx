import { useEffect, useMemo, useRef } from 'react'
import { Canvas, type ThreeEvent, useThree } from '@react-three/fiber'
import { OrbitControls, Text } from '@react-three/drei'
import * as THREE from 'three'

import type {
  RackBeam,
  RackLocation,
  RackPillar,
} from '@/features/warehouse3d/types'
import {
  AISLE_WIDTH,
  BEAM_HEIGHT,
  LEVEL_HEIGHT,
  LOCATION_DEPTH,
  LOCATION_HEIGHT,
  LOCATION_WIDTH,
  PILLAR_WIDTH,
  RACK_DEPTH,
  RACK_LEVELS,
  WAREHOUSE_AISLE_MARKERS,
  WAREHOUSE_BEAMS,
  WAREHOUSE_FACES,
  WAREHOUSE_LOCATION_LABELS,
  WAREHOUSE_PILLARS,
} from '@/features/warehouse3d/layout/warehouseLayout'

interface WarehouseSceneProps {
  locations: RackLocation[]
  selectedLocation: RackLocation | null
  showLabels: boolean
  showReferenceZones: boolean
  onSelectLocation: (location: RackLocation) => void
}

const locationColor = '#e2c371'
const selectedColor = '#f59e0b'
const structureColor = '#94a3b8'
const aisleColor = '#f8fafc'
const floorColor = '#e2e8f0'

const referenceZones = [
  { label: 'Almacenamiento suelo 1', x: -4.5, z: 13, width: 3.2, depth: 16 },
  { label: 'Almacenamiento suelo 2', x: -1.1, z: 13, width: 3.2, depth: 16 },
  {
    label: 'Playas recepcion/expedicion Mahou',
    x: 12,
    z: 29,
    width: 18,
    depth: 4.5,
  },
  {
    label: 'Playas recepcion/expedicion Red Bull',
    x: 32,
    z: 29,
    width: 13,
    depth: 4.5,
  },
  { label: 'Zona cross-docking', x: 48, z: 29, width: 6, depth: 4.5 },
]

const matrix = new THREE.Matrix4()

const getBounds = () => {
  const xs = [
    ...WAREHOUSE_FACES.map((face) => face.x),
    ...referenceZones.map((zone) => zone.x - zone.width / 2),
    ...referenceZones.map((zone) => zone.x + zone.width / 2),
  ]
  const zs = [
    ...WAREHOUSE_FACES.map((face) => face.zStart),
    ...WAREHOUSE_FACES.map((face) => face.zEnd),
    ...referenceZones.map((zone) => zone.z - zone.depth / 2),
    ...referenceZones.map((zone) => zone.z + zone.depth / 2),
  ]

  return {
    minX: Math.min(...xs) - 2,
    maxX: Math.max(...xs) + 2,
    minZ: Math.min(...zs) - 1.6,
    maxZ: Math.max(...zs) + 2,
  }
}

const CameraSetup = () => {
  const { camera } = useThree()

  useEffect(() => {
    camera.position.set(26, 19, 36)
    camera.lookAt(0, 1.8, 1.5)
    camera.updateProjectionMatrix()
  }, [camera])

  return null
}

const InstancedLocations = ({
  locations,
  onSelectLocation,
}: {
  locations: RackLocation[]
  onSelectLocation: (location: RackLocation) => void
}) => {
  const meshRef = useRef<THREE.InstancedMesh>(null)

  useEffect(() => {
    const mesh = meshRef.current

    if (!mesh) {
      return
    }

    locations.forEach((location, index) => {
      matrix.setPosition(location.x, location.y, location.z)
      mesh.setMatrixAt(index, matrix)
    })

    mesh.instanceMatrix.needsUpdate = true
  }, [locations])

  const handleClick = (event: ThreeEvent<MouseEvent>) => {
    event.stopPropagation()

    if (event.instanceId === undefined) {
      return
    }

    const location = locations[event.instanceId]

    if (location) {
      onSelectLocation(location)
    }
  }

  return (
    <instancedMesh
      ref={meshRef}
      args={[undefined, undefined, locations.length]}
      onClick={handleClick}
    >
      <boxGeometry args={[LOCATION_DEPTH, LOCATION_HEIGHT, LOCATION_WIDTH * 0.82]} />
      <meshStandardMaterial color={locationColor} roughness={0.72} metalness={0.08} />
    </instancedMesh>
  )
}

const InstancedPillars = ({ pillars }: { pillars: RackPillar[] }) => {
  const meshRef = useRef<THREE.InstancedMesh>(null)

  useEffect(() => {
    const mesh = meshRef.current

    if (!mesh) {
      return
    }

    pillars.forEach((pillar, index) => {
      matrix.setPosition(
        pillar.x,
        (RACK_LEVELS.length * LEVEL_HEIGHT) / 2,
        pillar.z,
      )
      mesh.setMatrixAt(index, matrix)
    })

    mesh.instanceMatrix.needsUpdate = true
  }, [pillars])

  return (
    <instancedMesh ref={meshRef} args={[undefined, undefined, pillars.length]}>
      <boxGeometry
        args={[RACK_DEPTH * 1.08, RACK_LEVELS.length * LEVEL_HEIGHT, PILLAR_WIDTH]}
      />
      <meshStandardMaterial
        color={structureColor}
        roughness={0.55}
        metalness={0.28}
        transparent
        opacity={0.78}
      />
    </instancedMesh>
  )
}

const InstancedBeams = ({ beams }: { beams: RackBeam[] }) => {
  const meshRef = useRef<THREE.InstancedMesh>(null)

  useEffect(() => {
    const mesh = meshRef.current

    if (!mesh) {
      return
    }

    beams.forEach((beam, index) => {
      matrix.compose(
        new THREE.Vector3(beam.x, beam.y, beam.z),
        new THREE.Quaternion(),
        new THREE.Vector3(RACK_DEPTH * 1.12, BEAM_HEIGHT, beam.length),
      )
      mesh.setMatrixAt(index, matrix)
    })

    mesh.instanceMatrix.needsUpdate = true
  }, [beams])

  return (
    <instancedMesh ref={meshRef} args={[undefined, undefined, beams.length]}>
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial
        color={structureColor}
        roughness={0.5}
        metalness={0.35}
        transparent
        opacity={0.62}
      />
    </instancedMesh>
  )
}

const AisleGround = () => (
  <group>
    {WAREHOUSE_AISLE_MARKERS.map((aisle) => (
      <group key={aisle.id}>
        <mesh position={[aisle.x, 0.005, aisle.z]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[AISLE_WIDTH, aisle.length]} />
          <meshStandardMaterial color={aisleColor} />
        </mesh>
        {[-0.8, aisle.z].map((zPosition) => (
          <Text
            key={`${aisle.id}-${zPosition}`}
            position={[aisle.x, 0.04, zPosition]}
            rotation={[-Math.PI / 2, 0, 0]}
            fontSize={0.62}
            fontWeight={700}
            color="#1e293b"
            anchorX="center"
            anchorY="middle"
          >
            {aisle.label}
          </Text>
        ))}
      </group>
    ))}
  </group>
)

const ReferenceZonePlates = () => (
  <group>
    {referenceZones.map((zone) => (
      <group key={zone.label} position={[zone.x, 0.018, zone.z]}>
        <mesh rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[zone.width, zone.depth]} />
          <meshStandardMaterial color="#cbd5e1" transparent opacity={0.36} />
        </mesh>
        <Text
          position={[0, 0.04, 0]}
          rotation={[-Math.PI / 2, 0, 0]}
          fontSize={0.32}
          maxWidth={zone.width - 0.6}
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

const LocationReferenceLabels = () => (
  <group>
    {WAREHOUSE_LOCATION_LABELS.map((label) => (
      <Text
        key={label.id}
        position={[label.x, label.y, label.z]}
        rotation={[-Math.PI / 5, 0, 0]}
        fontSize={0.18}
        color="#475569"
        anchorX="center"
        anchorY="middle"
      >
        {label.label}
      </Text>
    ))}
  </group>
)

export const WarehouseScene = ({
  locations,
  selectedLocation,
  showLabels,
  showReferenceZones,
  onSelectLocation,
}: WarehouseSceneProps) => {
  const bounds = useMemo(() => getBounds(), [])
  const centerX = (bounds.minX + bounds.maxX) / 2
  const centerZ = (bounds.minZ + bounds.maxZ) / 2
  const floorWidth = bounds.maxX - bounds.minX
  const floorDepth = bounds.maxZ - bounds.minZ
  const selectedIsVisible = locations.some(
    (location) => location.id === selectedLocation?.id,
  )

  return (
    <Canvas
      camera={{ position: [26, 19, 36], fov: 42, near: 0.1, far: 1000 }}
      gl={{ antialias: true, preserveDrawingBuffer: true }}
      onCreated={({ camera }) => camera.lookAt(0, 1.8, 1.5)}
      className="h-full w-full"
    >
      <color attach="background" args={['#f8fafc']} />
      <ambientLight intensity={0.74} />
      <directionalLight position={[8, 16, 12]} intensity={1.1} />

      <group position={[-centerX, 0, -centerZ]}>
        <mesh position={[centerX, -0.025, centerZ]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[floorWidth, floorDepth]} />
          <meshStandardMaterial color={floorColor} />
        </mesh>

        <AisleGround />
        {showReferenceZones && <ReferenceZonePlates />}

        <InstancedLocations locations={locations} onSelectLocation={onSelectLocation} />
        <InstancedPillars pillars={WAREHOUSE_PILLARS} />
        <InstancedBeams beams={WAREHOUSE_BEAMS} />

        {selectedLocation && selectedIsVisible && (
          <mesh position={[selectedLocation.x, selectedLocation.y, selectedLocation.z]}>
            <boxGeometry
              args={[LOCATION_DEPTH * 1.12, LOCATION_HEIGHT * 1.08, LOCATION_WIDTH * 0.96]}
            />
            <meshStandardMaterial color={selectedColor} roughness={0.5} metalness={0.06} />
          </mesh>
        )}

        {showLabels && <LocationReferenceLabels />}
      </group>

      <OrbitControls makeDefault target={[0, 1.8, 1.5]} maxPolarAngle={Math.PI / 2.08} />
      <CameraSetup />
    </Canvas>
  )
}
