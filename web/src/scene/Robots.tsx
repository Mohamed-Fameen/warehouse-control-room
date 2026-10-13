import { Html } from '@react-three/drei'
import { useFrame } from '@react-three/fiber'
import { useEffect, useMemo, useRef } from 'react'
import { BoxGeometry, Color, Group, InstancedMesh, Object3D } from 'three'
import { poseAt, renderTime, type Pose } from '../state/motion'
import { useFleet } from '../state/fleet'
import { STATUS_COLORS } from './colors'

const MAX_ROBOTS = 128
const dummy = new Object3D()
const pose: Pose = { x: 0, y: 0, heading: 0 }
const color = new Color()

/**
 * All robots as two instanced meshes (body and a nose showing heading). Instance i is
 * robot ids[i]; positions are interpolated every frame from the motion buffers.
 */
export function Robots() {
  const robots = useFleet((s) => s.robots)
  const selectedId = useFleet((s) => s.selectedId)
  const select = useFleet((s) => s.select)

  const body = useRef<InstancedMesh>(null!)
  const nose = useRef<InstancedMesh>(null!)
  const marker = useRef<Group>(null!)
  const ids = useMemo(() => Object.keys(robots).sort(), [robots])

  const bodyGeometry = useMemo(() => new BoxGeometry(0.8, 0.35, 0.8).translate(0, 0.2, 0), [])
  const noseGeometry = useMemo(() => new BoxGeometry(0.2, 0.1, 0.5).translate(0.3, 0.42, 0), [])

  useEffect(() => {
    ids.forEach((id, i) => body.current.setColorAt(i, color.set(STATUS_COLORS[robots[id].status])))
    if (body.current.instanceColor) body.current.instanceColor.needsUpdate = true
  }, [ids, robots])

  useFrame(() => {
    const t = renderTime()
    body.current.count = ids.length
    nose.current.count = ids.length
    ids.forEach((id, i) => {
      if (poseAt(id, t, pose)) {
        dummy.position.set(pose.x, 0, -pose.y)
        dummy.rotation.set(0, pose.heading, 0)
        dummy.scale.setScalar(1)
      } else {
        dummy.scale.setScalar(0) // no position known yet
      }
      dummy.updateMatrix()
      body.current.setMatrixAt(i, dummy.matrix)
      nose.current.setMatrixAt(i, dummy.matrix)
    })
    body.current.instanceMatrix.needsUpdate = true
    nose.current.instanceMatrix.needsUpdate = true
    body.current.computeBoundingSphere() // robots move, so raycasting needs a fresh one

    const shown = selectedId !== null && poseAt(selectedId, t, pose) !== null
    marker.current.visible = shown
    if (shown) marker.current.position.set(pose.x, 0, -pose.y)
  })

  const selected = selectedId ? robots[selectedId] : undefined

  return (
    <group>
      <instancedMesh
        ref={body}
        args={[bodyGeometry, undefined, MAX_ROBOTS]}
        castShadow
        onClick={(e) => {
          e.stopPropagation()
          if (e.instanceId !== undefined) select(ids[e.instanceId] ?? null)
        }}
        onPointerOver={() => (document.body.style.cursor = 'pointer')}
        onPointerOut={() => (document.body.style.cursor = '')}
      >
        <meshStandardMaterial roughness={0.4} />
      </instancedMesh>
      <instancedMesh ref={nose} args={[noseGeometry, undefined, MAX_ROBOTS]}>
        <meshStandardMaterial color="#e2e8f0" />
      </instancedMesh>
      <group ref={marker} visible={false}>
        <mesh rotation-x={-Math.PI / 2} position-y={0.02}>
          <ringGeometry args={[0.65, 0.8, 32]} />
          <meshBasicMaterial color="#f8fafc" />
        </mesh>
        {selected && (
          <Html position={[0, 1.1, 0]} center className="robot-label">
            {selected.id}
          </Html>
        )}
      </group>
    </group>
  )
}
