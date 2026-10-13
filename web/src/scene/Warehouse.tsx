import { Grid } from '@react-three/drei'
import { useLayoutEffect, useRef } from 'react'
import { InstancedMesh, Object3D } from 'three'
import type { Cell, Layout, Rect } from '../types'

// World mapping: warehouse x (east) -> three x, warehouse y (north) -> three -z, up is +y.

const RACK_HEIGHT = 2.4
const dummy = new Object3D()

function Racks({ racks }: { racks: Rect[] }) {
  const ref = useRef<InstancedMesh>(null!)
  useLayoutEffect(() => {
    racks.forEach((r, i) => {
      dummy.position.set(r.x + r.width / 2, RACK_HEIGHT / 2, -(r.y + r.height / 2))
      dummy.scale.set(r.width - 0.1, RACK_HEIGHT, r.height - 0.1)
      dummy.updateMatrix()
      ref.current.setMatrixAt(i, dummy.matrix)
    })
    ref.current.instanceMatrix.needsUpdate = true
    ref.current.computeBoundingSphere()
  }, [racks])
  return (
    <instancedMesh ref={ref} args={[undefined, undefined, racks.length]} castShadow receiveShadow>
      <boxGeometry />
      <meshStandardMaterial color="#7c5a3a" roughness={0.8} />
    </instancedMesh>
  )
}

interface PadsProps {
  cells: Cell[]
  size: number
  height: number
  color: string
  emissive: string
}

function Pads({ cells, size, height, color, emissive }: PadsProps) {
  const ref = useRef<InstancedMesh>(null!)
  useLayoutEffect(() => {
    cells.forEach((c, i) => {
      dummy.position.set(c.x + 0.5, height / 2, -(c.y + 0.5))
      dummy.scale.set(size, height, size)
      dummy.updateMatrix()
      ref.current.setMatrixAt(i, dummy.matrix)
    })
    ref.current.instanceMatrix.needsUpdate = true
    ref.current.computeBoundingSphere()
  }, [cells, size, height])
  return (
    <instancedMesh ref={ref} args={[undefined, undefined, cells.length]} receiveShadow>
      <boxGeometry />
      <meshStandardMaterial color={color} emissive={emissive} emissiveIntensity={0.4} />
    </instancedMesh>
  )
}

export function Warehouse({ layout }: { layout: Layout }) {
  const { width, height } = layout
  return (
    <group>
      <mesh rotation-x={-Math.PI / 2} position={[width / 2, -0.01, -height / 2]} receiveShadow>
        <planeGeometry args={[width, height]} />
        <meshStandardMaterial color="#1e293b" />
      </mesh>
      <Grid
        position={[width / 2, 0, -height / 2]}
        args={[width, height]}
        cellSize={1}
        cellColor="#334155"
        cellThickness={0.6}
        sectionSize={4}
        sectionColor="#475569"
        sectionThickness={1}
        fadeDistance={200}
      />
      <Racks racks={layout.racks} />
      <Pads cells={layout.chargers} size={0.7} height={0.04} color="#166534" emissive="#22c55e" />
      <Pads cells={layout.packStations} size={0.95} height={0.9} color="#1d4ed8" emissive="#1e40af" />
    </group>
  )
}
