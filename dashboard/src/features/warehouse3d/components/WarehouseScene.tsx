import { memo, useEffect, useLayoutEffect, useMemo, useRef } from 'react'
import { Canvas, type ThreeEvent, useThree } from '@react-three/fiber'
import { Edges, OrbitControls, Text } from '@react-three/drei'
import * as THREE from 'three'

import { RackBay3EU } from '@/features/warehouse3d/components/RackBay3EU'
import type {
  RackBayTypeOverrides,
  RackLocation,
} from '@/features/warehouse3d/types'
import {
  AISLE_WIDTH,
  BAY_WIDTH,
  BEAM_DEPTH,
  BEAM_HEIGHT,
  getBayType,
  getLocationZ,
  LOCATION_HEIGHT,
  LOCATION_WIDTH,
  POST_HEIGHT,
  POST_WIDTH,
  RACK_DEPTH,
  RACK_FRAME_POST_OFFSET,
  SLOT_DEPTH,
  SPLIT_LOCATION_HEIGHT,
  SPLIT_MIDDLE_BEAM_Y,
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
  rackBayTypeOverrides: RackBayTypeOverrides
  editRackTypes: boolean
  onSelectLocation: (location: RackLocation) => void
  onEditRackBay: (bayId: string) => void
}

const aisleColor = '#ffffff'
const floorColor = '#e5e7eb'
const selectedFloorColor = '#dbeafe'
const postColor = '#0073e6'
const beamColor = '#ef1d16'
const emptyLocationColor = '#e5e7eb'
const emptyEdgeColor = '#ffffff'
const selectedLocationColor = '#f59e0b'
const selectedEdgeColor = '#fff7ed'
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
  const { camera, invalidate } = useThree()

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
    invalidate()
  }, [camera, debugSingleBay, invalidate])

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

const setInstanceMatrices = (
  mesh: THREE.InstancedMesh | null,
  matrices: THREE.Matrix4[],
) => {
  if (!mesh) {
    return
  }

  matrices.forEach((matrix, index) => mesh.setMatrixAt(index, matrix))
  mesh.instanceMatrix.setUsage(THREE.StaticDrawUsage)
  mesh.instanceMatrix.needsUpdate = true
  mesh.computeBoundingSphere()
}

const InstancedLocationLayer = memo(({
  locations,
  height,
  onLocationClick,
}: {
  locations: RackLocation[]
  height: number
  onLocationClick: (location: RackLocation) => void
}) => {
  const locationMeshRef = useRef<THREE.InstancedMesh>(null)
  const wireframeMeshRef = useRef<THREE.InstancedMesh>(null)
  const { invalidate } = useThree()
  const matrices = useMemo(
    () =>
      locations.map((location) =>
        new THREE.Matrix4().makeTranslation(location.x, location.y, location.z),
      ),
    [locations],
  )

  useLayoutEffect(() => {
    setInstanceMatrices(locationMeshRef.current, matrices)
    setInstanceMatrices(wireframeMeshRef.current, matrices)
    invalidate()
  }, [invalidate, matrices])

  const handleClick = (event: ThreeEvent<MouseEvent>) => {
    event.stopPropagation()

    if (event.instanceId === undefined) {
      return
    }

    const location = locations[event.instanceId]

    if (location) {
      onLocationClick(location)
    }
  }

  if (locations.length === 0) {
    return null
  }

  return (
    <group>
      <instancedMesh
        ref={locationMeshRef}
        args={[undefined, undefined, locations.length]}
        onClick={handleClick}
      >
        <boxGeometry args={[SLOT_DEPTH, height, LOCATION_WIDTH]} />
        <meshStandardMaterial
          color={emptyLocationColor}
          roughness={0.55}
          metalness={0.02}
          transparent
          opacity={0.38}
          depthWrite={false}
        />
      </instancedMesh>

      <instancedMesh
        ref={wireframeMeshRef}
        args={[undefined, undefined, locations.length]}
        raycast={() => null}
      >
        <boxGeometry
          args={[
            SLOT_DEPTH * 1.002,
            height * 1.002,
            LOCATION_WIDTH * 1.002,
          ]}
        />
        <meshBasicMaterial
          color={emptyEdgeColor}
          wireframe
          transparent
          opacity={0.5}
          depthWrite={false}
        />
      </instancedMesh>
    </group>
  )
})

InstancedLocationLayer.displayName = 'InstancedLocationLayer'

const InstancedRackWarehouse = memo(({
  locations,
  selectedLocation,
  rackBayTypeOverrides,
  editRackTypes,
  onSelectLocation,
  onEditRackBay,
}: {
  locations: RackLocation[]
  selectedLocation: RackLocation | null
  rackBayTypeOverrides: RackBayTypeOverrides
  editRackTypes: boolean
  onSelectLocation: (location: RackLocation) => void
  onEditRackBay: (bayId: string) => void
}) => {
  const postMeshRef = useRef<THREE.InstancedMesh>(null)
  const beamMeshRef = useRef<THREE.InstancedMesh>(null)
  const middleBeamMeshRef = useRef<THREE.InstancedMesh>(null)
  const { invalidate } = useThree()

  const { standardLocations, splitBottomLocations, splitTopLocations } = useMemo(() => {
    const standard: RackLocation[] = []
    const splitBottom: RackLocation[] = []
    const splitTop: RackLocation[] = []

    locations.forEach((location) => {
      if (getBayType(location.bayId, rackBayTypeOverrides) === 'standard-3eu') {
        standard.push(location)
      } else if (location.level === 0) {
        splitBottom.push(location)
      } else {
        splitTop.push(location)
      }
    })

    return {
      standardLocations: standard,
      splitBottomLocations: splitBottom,
      splitTopLocations: splitTop,
    }
  }, [locations, rackBayTypeOverrides])

  const { postMatrices, beamMatrices } = useMemo(() => {
    const posts: THREE.Matrix4[] = []
    const beams: THREE.Matrix4[] = []
    const postZOffset = BAY_WIDTH / 2 + POST_WIDTH / 2
    const beamYTop = LOCATION_HEIGHT + 0.34

    WAREHOUSE_BAYS.forEach((bay) => {
      const frontX = bay.x + bay.rackDepthSign * RACK_FRAME_POST_OFFSET
      const rearX = bay.x - bay.rackDepthSign * RACK_FRAME_POST_OFFSET

      for (const x of [frontX, rearX]) {
        for (const zOffset of [-postZOffset, postZOffset]) {
          posts.push(
            new THREE.Matrix4().makeTranslation(
              x,
              POST_HEIGHT / 2,
              bay.z + zOffset,
            ),
          )
        }

        beams.push(new THREE.Matrix4().makeTranslation(x, beamYTop, bay.z))
      }
    })

    return { postMatrices: posts, beamMatrices: beams }
  }, [])

  const middleBeamMatrices = useMemo(() => {
    const matrices: THREE.Matrix4[] = []
    WAREHOUSE_BAYS.forEach((bay) => {
      if (getBayType(bay.id, rackBayTypeOverrides) !== 'split-6eu') {
        return
      }

      const frontX = bay.x + bay.rackDepthSign * RACK_FRAME_POST_OFFSET
      const rearX = bay.x - bay.rackDepthSign * RACK_FRAME_POST_OFFSET

      matrices.push(
        new THREE.Matrix4().makeTranslation(frontX, SPLIT_MIDDLE_BEAM_Y, bay.z),
        new THREE.Matrix4().makeTranslation(rearX, SPLIT_MIDDLE_BEAM_Y, bay.z),
      )
    })

    return matrices
  }, [rackBayTypeOverrides])

  useLayoutEffect(() => {
    setInstanceMatrices(postMeshRef.current, postMatrices)
    setInstanceMatrices(beamMeshRef.current, beamMatrices)
    setInstanceMatrices(middleBeamMeshRef.current, middleBeamMatrices)
    invalidate()
  }, [beamMatrices, invalidate, middleBeamMatrices, postMatrices])

  const handleLocationClick = (location: RackLocation) => {
    if (editRackTypes) {
      onEditRackBay(location.bayId)
    } else {
      onSelectLocation(location)
    }
  }

  const selectedLocationHeight =
    selectedLocation &&
    getBayType(selectedLocation.bayId, rackBayTypeOverrides) === 'split-6eu'
      ? SPLIT_LOCATION_HEIGHT
      : LOCATION_HEIGHT

  return (
    <group>
      <InstancedLocationLayer
        locations={standardLocations}
        height={LOCATION_HEIGHT}
        onLocationClick={handleLocationClick}
      />
      <InstancedLocationLayer
        locations={splitBottomLocations}
        height={SPLIT_LOCATION_HEIGHT}
        onLocationClick={handleLocationClick}
      />
      <InstancedLocationLayer
        locations={splitTopLocations}
        height={SPLIT_LOCATION_HEIGHT}
        onLocationClick={handleLocationClick}
      />

      <instancedMesh
        ref={postMeshRef}
        args={[undefined, undefined, postMatrices.length]}
        raycast={() => null}
      >
        <boxGeometry args={[POST_WIDTH, POST_HEIGHT, POST_WIDTH]} />
        <meshStandardMaterial color={postColor} roughness={0.38} metalness={0.25} />
      </instancedMesh>

      <instancedMesh
        ref={beamMeshRef}
        args={[undefined, undefined, beamMatrices.length]}
        raycast={() => null}
      >
        <boxGeometry args={[BEAM_DEPTH, BEAM_HEIGHT, BAY_WIDTH + POST_WIDTH]} />
        <meshStandardMaterial color={beamColor} roughness={0.34} metalness={0.18} />
      </instancedMesh>

      {middleBeamMatrices.length > 0 && (
        <instancedMesh
          ref={middleBeamMeshRef}
          args={[undefined, undefined, middleBeamMatrices.length]}
          raycast={() => null}
        >
          <boxGeometry args={[BEAM_DEPTH, BEAM_HEIGHT, BAY_WIDTH + POST_WIDTH]} />
          <meshStandardMaterial color={beamColor} roughness={0.34} metalness={0.18} />
        </instancedMesh>
      )}

      {selectedLocation && (
        <mesh
          position={[selectedLocation.x, selectedLocation.y, selectedLocation.z]}
          raycast={() => null}
        >
          <boxGeometry
            args={[SLOT_DEPTH, selectedLocationHeight, LOCATION_WIDTH]}
          />
          <meshStandardMaterial
            color={selectedLocationColor}
            roughness={0.55}
            metalness={0.02}
            transparent
            opacity={0.86}
          />
          <Edges scale={1.002} threshold={12} color={selectedEdgeColor} />
        </mesh>
      )}
    </group>
  )
})

InstancedRackWarehouse.displayName = 'InstancedRackWarehouse'

const DebugSingleBay = ({
  selectedLocationUid,
  onSelectLocation,
}: {
  selectedLocationUid?: string | null
  onSelectLocation: (location: RackLocation) => void
}) => {
  const locations = WAREHOUSE_LOCATIONS.filter(
    (location) => location.aisle === 20 && location.side === 'PAR',
  ).slice(0, 3).map((location) => ({
    ...location,
    x: 0,
    z: location.z - getLocationZ(4),
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
  rackBayTypeOverrides,
  editRackTypes,
  onSelectLocation,
  onEditRackBay,
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
      frameloop="demand"
      dpr={[1, 1.5]}
      orthographic
      camera={{
        position: debugSingleBay ? [4, 3, 5] : FULL_CAMERA_POSITION,
        zoom: debugSingleBay ? 90 : FULL_CAMERA_ZOOM,
        near: 0.1,
        far: 1000,
      }}
      gl={{ antialias: true }}
      style={{ cursor: editRackTypes ? 'pointer' : undefined }}
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

          <InstancedRackWarehouse
            locations={locations}
            selectedLocation={selectedLocation}
            rackBayTypeOverrides={rackBayTypeOverrides}
            editRackTypes={editRackTypes}
            onSelectLocation={onSelectLocation}
            onEditRackBay={onEditRackBay}
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
        zoomSpeed={1.1}
        panSpeed={0.7}
        screenSpacePanning={false}
        minPolarAngle={0.22}
        maxPolarAngle={1.32}
        minZoom={debugSingleBay ? 35 : 2.5}
        maxZoom={debugSingleBay ? 140 : 60}
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
