import { Edges } from '@react-three/drei'
import type { ThreeEvent } from '@react-three/fiber'

import type { RackLocation } from '@/features/warehouse3d/types'
import {
  BAY_WIDTH,
  BEAM_DEPTH,
  BEAM_HEIGHT,
  LOCATION_HEIGHT,
  LOCATION_WIDTH,
  POST_HEIGHT,
  POST_WIDTH,
  RACK_FRAME_POST_OFFSET,
  SLOT_DEPTH,
} from '@/features/warehouse3d/layout/warehouseLayout'

type Vec3 = [number, number, number]

interface RackBay3EUProps {
  bayId: string
  locations: RackLocation[]
  position?: Vec3
  rotation?: Vec3
  rackDepthSign?: number
  selectedLocationUid?: string | null
  onSelectLocation: (location: RackLocation) => void
}

const postColor = '#0073e6'
const beamColor = '#ef1d16'
const emptyLocationColor = '#e5e7eb'
const emptyEdgeColor = '#ffffff'
const selectedLocationColor = '#f59e0b'
const selectedEdgeColor = '#fff7ed'

const postZOffset = BAY_WIDTH / 2 + POST_WIDTH / 2
const beamYTop = LOCATION_HEIGHT + 0.34

export const RackBay3EU = ({
  bayId,
  locations,
  position = [0, 0, 0],
  rotation = [0, 0, 0],
  rackDepthSign = 1,
  selectedLocationUid,
  onSelectLocation,
}: RackBay3EUProps) => {
  const frontX = rackDepthSign * RACK_FRAME_POST_OFFSET
  const rearX = -rackDepthSign * RACK_FRAME_POST_OFFSET
  const postPositions: Vec3[] = [
    [frontX, POST_HEIGHT / 2, -postZOffset],
    [frontX, POST_HEIGHT / 2, postZOffset],
    [rearX, POST_HEIGHT / 2, -postZOffset],
    [rearX, POST_HEIGHT / 2, postZOffset],
  ]
  const beamPositions: Vec3[] = [
    [frontX, beamYTop, 0],
    [rearX, beamYTop, 0],
  ]

  const handleLocationClick = (
    event: ThreeEvent<MouseEvent>,
    location: RackLocation,
  ) => {
    event.stopPropagation()
    onSelectLocation(location)
  }

  return (
    <group name={bayId} position={position} rotation={rotation}>
      {postPositions.map((postPosition, index) => (
        <mesh
          key={`${bayId}-post-${index}`}
          position={postPosition}
          raycast={() => null}
          castShadow
        >
          <boxGeometry args={[POST_WIDTH, POST_HEIGHT, POST_WIDTH]} />
          <meshStandardMaterial color={postColor} roughness={0.38} metalness={0.25} />
        </mesh>
      ))}

      {beamPositions.map((beamPosition, index) => (
        <mesh
          key={`${bayId}-beam-${index}`}
          position={beamPosition}
          raycast={() => null}
          castShadow
        >
          <boxGeometry args={[BEAM_DEPTH, BEAM_HEIGHT, BAY_WIDTH + POST_WIDTH]} />
          <meshStandardMaterial color={beamColor} roughness={0.34} metalness={0.18} />
        </mesh>
      ))}

      {locations.map((location) => {
        const selected = location.uid === selectedLocationUid
        const locationPosition: Vec3 = [
          location.x - position[0],
          location.y - position[1],
          location.z - position[2],
        ]

        return (
          <mesh
            key={location.uid}
            position={locationPosition}
            onClick={(event) => handleLocationClick(event, location)}
          >
            <boxGeometry
              args={[SLOT_DEPTH, LOCATION_HEIGHT, LOCATION_WIDTH]}
            />
            <meshStandardMaterial
              color={selected ? selectedLocationColor : emptyLocationColor}
              roughness={0.55}
              metalness={0.02}
              transparent
              opacity={selected ? 0.86 : 0.38}
              depthWrite={selected}
            />
            <Edges
              scale={1.002}
              threshold={12}
              color={selected ? selectedEdgeColor : emptyEdgeColor}
            />
          </mesh>
        )
      })}
    </group>
  )
}
