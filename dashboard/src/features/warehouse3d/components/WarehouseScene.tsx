import { useEffect, useMemo } from 'react'
import { Canvas, useThree } from '@react-three/fiber'
import { OrbitControls, Text } from '@react-three/drei'
import * as THREE from 'three'

import { RackBay3EU } from '@/features/warehouse3d/components/RackBay3EU'
import type { RackLocation } from '@/features/warehouse3d/types'
import {
  AISLE_WIDTH,
  getLocationZ,
  LOCATION_WIDTH,
  RACK_DEPTH,
  WAREHOUSE_AISLE_MARKERS,
  WAREHOUSE_BAYS,
  WAREHOUSE_FACES,
  WAREHOUSE_LOCATION_LABELS,
  WAREHOUSE_LOCATIONS,
  WAREHOUSE_REFERENCE_ZONES,
} from '@/features/warehouse3d/layout/warehouseLayout'

interface WarehouseSceneProps {
  locations: RackLocation[]
  selectedLocation: RackLocation | null
  showLabels: boolean
  showReferenceZones: boolean
  onSelectLocation: (location: RackLocation) => void
}

const aisleColor = '#ffffff'
const floorColor = '#e5e7eb'
const selectedFloorColor = '#dbeafe'
const FULL_CAMERA_POSITION: [number, number, number] = [0, 125, -75]
const FULL_CAMERA_ZOOM = 5.8

const missingZoneStartZ = getLocationZ(1) - LOCATION_WIDTH / 2

const getZoneFootprint = (zoneId: string) => {
  const faces = WAREHOUSE_FACES.filter((face) => face.zoneId === zoneId)
  const minX = Math.min(...faces.map((face) => face.x)) - RACK_DEPTH
  const maxX = Math.max(...faces.map((face) => face.x)) + RACK_DEPTH
  const minZ = Math.min(...faces.map((face) => face.zStart)) - LOCATION_WIDTH
  const maxZ = Math.max(...faces.map((face) => face.zEnd)) + LOCATION_WIDTH

  return {
    x: (minX + maxX) / 2,
    z: (minZ + maxZ) / 2,
    width: maxX - minX,
    depth: maxZ - minZ,
  }
}

const zoneFootprints = [
  {
    id: 'zone-a',
    label: 'Zona A parcial U037-U120',
    color: '#bfdbfe',
    ...getZoneFootprint('zone-a'),
  },
  {
    id: 'zone-b',
    label: 'Zona B completa U001-U120',
    color: '#bbf7d0',
    ...getZoneFootprint('zone-b'),
  },
]

const getBounds = () => {
  const xs = [
    ...WAREHOUSE_FACES.map((face) => face.x),
    ...WAREHOUSE_REFERENCE_ZONES.map((zone) => zone.x - zone.width / 2),
    ...WAREHOUSE_REFERENCE_ZONES.map((zone) => zone.x + zone.width / 2),
  ]
  const zs = [
    ...WAREHOUSE_FACES.map((face) => face.zStart),
    ...WAREHOUSE_FACES.map((face) => face.zEnd),
    ...WAREHOUSE_REFERENCE_ZONES.map((zone) => zone.z - zone.depth / 2),
    ...WAREHOUSE_REFERENCE_ZONES.map((zone) => zone.z + zone.depth / 2),
  ]

  return {
    minX: Math.min(...xs) - 2,
    maxX: Math.max(...xs) + 2,
    minZ: Math.min(...zs) - 2,
    maxZ: Math.max(...zs) + 2,
  }
}

const CameraSetup = ({ debugSingleBay }: { debugSingleBay: boolean }) => {
  const { camera } = useThree()

  useEffect(() => {
    camera.up.set(0, 1, 0)

    if (debugSingleBay) {
      camera.position.set(4, 3, 5)
      camera.lookAt(0, 0.9, 0)
    } else {
      camera.position.set(...FULL_CAMERA_POSITION)
      camera.lookAt(0, 0, 0)
    }

    camera.updateProjectionMatrix()
  }, [camera, debugSingleBay])

  return null
}

const AisleGround = () => (
  <group>
    {WAREHOUSE_AISLE_MARKERS.map((aisle) => (
      <group key={aisle.id}>
        <mesh
          position={[aisle.x, 0.005, aisle.z]}
          rotation={[-Math.PI / 2, 0, 0]}
          raycast={() => null}
        >
          <planeGeometry args={[AISLE_WIDTH, aisle.length]} />
          <meshStandardMaterial color={aisleColor} />
        </mesh>
        {[missingZoneStartZ - 1.4, getLocationZ(120) + 1.2].map((zPosition) => (
          <Text
            key={`${aisle.id}-${zPosition}`}
            position={[aisle.x, 0.04, zPosition]}
            rotation={[-Math.PI / 2, 0, Math.PI]}
            scale={[-1, 1, 1]}
            fontSize={1}
            fontWeight={700}
            color="#111827"
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

const ZoneFootprints = () => (
  <group>
    {zoneFootprints.map((zone) => (
      <group key={zone.id} position={[zone.x, 0.002, zone.z]}>
        <mesh rotation={[-Math.PI / 2, 0, 0]} raycast={() => null}>
          <planeGeometry args={[zone.width, zone.depth]} />
          <meshStandardMaterial color={zone.color} transparent opacity={0.24} />
        </mesh>
        <Text
          position={[0, 0.035, 0]}
          rotation={[-Math.PI / 2, 0, Math.PI]}
          scale={[-1, 1, 1]}
          fontSize={1}
          fontWeight={700}
          color="#334155"
          anchorX="center"
          anchorY="middle"
        >
          {zone.label}
        </Text>
      </group>
    ))}
  </group>
)

const ReferenceZonePlates = () => (
  <group>
    {WAREHOUSE_REFERENCE_ZONES.map((zone) => (
      <group key={zone.id} position={[zone.x, 0.018, zone.z]}>
        <mesh rotation={[-Math.PI / 2, 0, 0]} raycast={() => null}>
          <planeGeometry args={[zone.width, zone.depth]} />
          <meshStandardMaterial color="#94a3b8" transparent opacity={0.58} />
        </mesh>
        <Text
          position={[0, 0.04, 0]}
          rotation={[-Math.PI / 2, 0, Math.PI]}
          scale={[-1, 1, 1]}
          fontSize={0.78}
          fontWeight={700}
          maxWidth={zone.width - 0.6}
          textAlign="center"
          color="#111827"
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
        rotation={[-Math.PI / 2, 0, Math.PI]}
        scale={[-1, 1, 1]}
        fontSize={0.38}
        fontWeight={700}
        color="#475569"
        anchorX="center"
        anchorY="middle"
      >
        {label.label}
      </Text>
    ))}
  </group>
)

const RackBayCollection = ({
  locations,
  selectedLocationUid,
  onSelectLocation,
}: {
  locations: RackLocation[]
  selectedLocationUid?: string | null
  onSelectLocation: (location: RackLocation) => void
}) => {
  const locationsByBay = useMemo(() => {
    const map = new Map<string, RackLocation[]>()

    locations.forEach((location) => {
      const bayLocations = map.get(location.bayId) ?? []
      bayLocations.push(location)
      map.set(location.bayId, bayLocations)
    })

    map.forEach((bayLocations) => {
      bayLocations.sort((a, b) => a.location - b.location)
    })

    return map
  }, [locations])

  return (
    <group>
      {WAREHOUSE_BAYS.map((bay) => {
        const bayLocations = locationsByBay.get(bay.id)

        if (!bayLocations?.length) {
          return null
        }

        return (
          <RackBay3EU
            key={bay.id}
            bayId={bay.id}
            locations={bayLocations}
            position={[bay.x, 0, bay.z]}
            rackDepthSign={bay.rackDepthSign}
            selectedLocationUid={selectedLocationUid}
            onSelectLocation={onSelectLocation}
          />
        )
      })}
    </group>
  )
}

const DebugSingleBay = ({
  selectedLocationUid,
  onSelectLocation,
}: {
  selectedLocationUid?: string | null
  onSelectLocation: (location: RackLocation) => void
}) => {
  const locations = WAREHOUSE_LOCATIONS.filter(
    (location) =>
      location.aisle === 20 &&
      location.side === 'PAR' &&
      location.location >= 1 &&
      location.location <= 3,
  ).map((location) => ({
    ...location,
    x: 0,
    z: location.z - getLocationZ(2),
  }))

  return (
    <group>
      <mesh position={[0, -0.025, 0]} rotation={[-Math.PI / 2, 0, 0]} raycast={() => null}>
        <planeGeometry args={[5, 5]} />
        <meshStandardMaterial color={selectedFloorColor} />
      </mesh>
      <RackBay3EU
        bayId="debug-rack-bay-3eu"
        locations={locations}
        position={[0, 0, 0]}
        rackDepthSign={-1}
        selectedLocationUid={selectedLocationUid}
        onSelectLocation={onSelectLocation}
      />
    </group>
  )
}

export const WarehouseScene = ({
  locations,
  selectedLocation,
  showLabels,
  showReferenceZones,
  onSelectLocation,
}: WarehouseSceneProps) => {
  const debugSingleBay = useMemo(
    () =>
      typeof window !== 'undefined' &&
      new URLSearchParams(window.location.search).get('warehouse3dDebug') === 'bay',
    [],
  )
  const bounds = useMemo(() => getBounds(), [])
  const centerX = (bounds.minX + bounds.maxX) / 2
  const centerZ = (bounds.minZ + bounds.maxZ) / 2
  const floorWidth = bounds.maxX - bounds.minX
  const floorDepth = bounds.maxZ - bounds.minZ

  return (
    <Canvas
      orthographic
      camera={{
        position: debugSingleBay ? [4, 3, 5] : FULL_CAMERA_POSITION,
        zoom: debugSingleBay ? 90 : FULL_CAMERA_ZOOM,
        near: 0.1,
        far: 1000,
      }}
      gl={{ antialias: true, preserveDrawingBuffer: true }}
      onCreated={({ camera }) => {
        camera.up.set(0, 1, 0)
        camera.lookAt(debugSingleBay ? new THREE.Vector3(0, 0.9, 0) : new THREE.Vector3(0, 0, 0))
      }}
      className="h-full w-full"
    >
      <color attach="background" args={['#f8fafc']} />
      <ambientLight intensity={0.72} />
      <directionalLight position={[7, 12, -8]} intensity={1.05} />
      <directionalLight position={[-8, 8, 10]} intensity={0.45} />

      {debugSingleBay ? (
        <DebugSingleBay
          selectedLocationUid={selectedLocation?.uid}
          onSelectLocation={onSelectLocation}
        />
      ) : (
        <group position={[centerX, 0, -centerZ]} scale={[-1, 1, 1]}>
          <mesh
            position={[centerX, -0.035, centerZ]}
            rotation={[-Math.PI / 2, 0, 0]}
            raycast={() => null}
          >
            <planeGeometry args={[floorWidth, floorDepth]} />
            <meshStandardMaterial color={floorColor} />
          </mesh>

          <AisleGround />
          <ZoneFootprints />
          {showReferenceZones && <ReferenceZonePlates />}

          <RackBayCollection
            locations={locations}
            selectedLocationUid={selectedLocation?.uid}
            onSelectLocation={onSelectLocation}
          />

          {showLabels && <LocationReferenceLabels />}
        </group>
      )}

      <OrbitControls
        makeDefault
        target={debugSingleBay ? [0, 0.9, 0] : [0, 0, 0]}
        enableRotate
        enablePan
        enableZoom
        enableDamping
        dampingFactor={0.08}
        rotateSpeed={0.62}
        zoomSpeed={0.8}
        panSpeed={0.7}
        screenSpacePanning={false}
        minPolarAngle={0.22}
        maxPolarAngle={1.32}
        minZoom={debugSingleBay ? 35 : 2.5}
        maxZoom={debugSingleBay ? 140 : 12}
        mouseButtons={{
          LEFT: THREE.MOUSE.ROTATE,
          MIDDLE: THREE.MOUSE.DOLLY,
          RIGHT: THREE.MOUSE.PAN,
        }}
      />
      <CameraSetup debugSingleBay={debugSingleBay} />
    </Canvas>
  )
}
