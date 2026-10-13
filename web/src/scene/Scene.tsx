import { OrbitControls } from '@react-three/drei'
import { Canvas } from '@react-three/fiber'
import { useFleet } from '../state/fleet'
import type { Layout } from '../types'
import { Robots } from './Robots'
import { Warehouse } from './Warehouse'

export function Scene({ layout }: { layout: Layout }) {
  const select = useFleet((s) => s.select)
  const cx = layout.width / 2
  const cz = -layout.height / 2
  return (
    <Canvas
      shadows="percentage"
      camera={{ position: [cx + 4, 36, cz + 28], fov: 45, near: 0.5, far: 500 }}
      onPointerMissed={() => select(null)}
    >
      <color attach="background" args={['#0b1120']} />
      <ambientLight intensity={0.6} />
      <directionalLight
        position={[cx - 15, 30, cz + 20]}
        intensity={1.6}
        castShadow
        shadow-mapSize={[2048, 2048]}
        shadow-camera-left={-30}
        shadow-camera-right={30}
        shadow-camera-top={30}
        shadow-camera-bottom={-30}
        shadow-camera-far={120}
        onUpdate={(light) => {
          light.target.position.set(cx, 0, cz)
          light.target.updateMatrixWorld()
        }}
      />
      <Warehouse layout={layout} />
      <Robots />
      <OrbitControls target={[cx + 4, 0, cz]} maxPolarAngle={Math.PI / 2.2} minDistance={8} maxDistance={90} makeDefault />
    </Canvas>
  )
}
