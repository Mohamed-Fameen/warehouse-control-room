import type { Cell, Layout, Rect, Robot, RobotStatus } from '../types'
import type { FleetFeed, FleetHandlers } from './fleetSocket'

// In-browser stand-in for the gateway, enabled with ?mock. It speaks the same protocol
// (snapshot, then deltas every 100 ms with robots reporting at 5 Hz) so the UI can be
// worked on without the backend running. Robots wander the aisles; they do not do real jobs.

/** Same floor plan as WarehouseGrid.standard() in services/common. */
export function mockLayout(): Layout {
  const width = 40
  const height = 26
  const racks: Rect[] = []
  for (let x0 = 4; x0 + 1 < width - 4; x0 += 4) {
    for (const y0 of [4, 14]) racks.push({ x: x0, y: y0, width: 2, height: 8 })
  }
  const chargers: Cell[] = []
  for (let x = 2; x < width - 1; x += 2) chargers.push({ x, y: 1 })
  for (let y = 4; y <= 21; y += 2) {
    chargers.push({ x: 1, y })
    chargers.push({ x: width - 2, y })
  }
  const packStations: Cell[] = []
  for (let x = 4; x < width; x += 6) packStations.push({ x, y: height - 2 })
  return { width, height, racks, chargers, packStations }
}

const SPEED = 1.5 // m/s
const DIRS: Cell[] = [
  { x: 1, y: 0 },
  { x: -1, y: 0 },
  { x: 0, y: 1 },
  { x: 0, y: -1 },
]

interface MockRobot {
  view: Robot
  cell: Cell
  target: Cell
  dir: Cell
  pauseUntil: number
}

export function connectMockFleet(handlers: FleetHandlers, robotCount = 20): FleetFeed {
  const layout = mockLayout()
  const shelf = new Set<string>()
  for (const r of layout.racks) {
    for (let x = r.x; x < r.x + r.width; x++) {
      for (let y = r.y; y < r.y + r.height; y++) shelf.add(`${x},${y}`)
    }
  }
  const walkable = (c: Cell) =>
    c.x >= 0 && c.y >= 0 && c.x < layout.width && c.y < layout.height && !shelf.has(`${c.x},${c.y}`)

  const now = Date.now()
  const robots: MockRobot[] = []
  for (let i = 0; i < robotCount; i++) {
    let cell: Cell
    do {
      cell = { x: Math.floor(Math.random() * layout.width), y: Math.floor(Math.random() * layout.height) }
    } while (!walkable(cell))
    const status: RobotStatus = i === robotCount - 1 ? 'OFFLINE' : i === robotCount - 2 ? 'BROKEN' : 'MOVING'
    const known = status !== 'OFFLINE'
    robots.push({
      view: {
        id: `robot-${String(i + 1).padStart(2, '0')}`,
        x: known ? cell.x + 0.5 : null,
        y: known ? cell.y + 0.5 : null,
        heading: known ? 0 : null,
        speed: known ? 0 : null,
        battery: known ? 40 + Math.random() * 60 : null,
        status,
        lastSeen: known ? now : null,
        statusTs: now,
      },
      cell,
      target: cell,
      dir: DIRS[i % 4],
      pauseUntil: 0,
    })
  }

  const step = (r: MockRobot, dt: number, t: number) => {
    const v = r.view
    if (v.status === 'OFFLINE' || v.status === 'BROKEN' || v.x === null || v.y === null) return
    const battery = Math.min(100, Math.max(0, (v.battery ?? 100) - dt * (v.status === 'CHARGING' ? -4 : 0.3)))
    v.battery = battery
    if (t < r.pauseUntil) return
    if (v.status === 'CHARGING' && battery < 95) return
    if (battery < 15) {
      v.status = 'CHARGING'
      v.speed = 0
      return
    }
    v.status = 'MOVING'
    const tx = r.target.x + 0.5
    const ty = r.target.y + 0.5
    const dx = tx - v.x
    const dy = ty - v.y
    const dist = Math.hypot(dx, dy)
    const move = SPEED * dt
    if (dist > move) {
      v.x += (dx / dist) * move
      v.y += (dy / dist) * move
      v.heading = Math.atan2(dy, dx)
      v.speed = SPEED
      return
    }
    v.x = tx
    v.y = ty
    r.cell = r.target
    if (Math.random() < 0.04) {
      v.status = 'PICKING'
      v.speed = 0
      r.pauseUntil = t + 1500 + Math.random() * 2000
      return
    }
    // Mostly keep going straight; turn at random or when blocked.
    const options = DIRS.filter(
      (d) => walkable({ x: r.cell.x + d.x, y: r.cell.y + d.y }) && !(d.x === -r.dir.x && d.y === -r.dir.y),
    )
    const ahead = options.find((d) => d === r.dir)
    r.dir =
      ahead && Math.random() < 0.8
        ? ahead
        : (options[Math.floor(Math.random() * options.length)] ?? DIRS.find((d) => d.x === -r.dir.x && d.y === -r.dir.y)!)
    r.target = { x: r.cell.x + r.dir.x, y: r.cell.y + r.dir.y }
  }

  const sim = window.setInterval(() => {
    const t = Date.now()
    robots.forEach((r) => step(r, 0.05, t))
  }, 50)

  // Each robot reports every 200 ms, staggered; the "gateway" batches every 100 ms.
  let tick = 0
  const report = window.setInterval(() => {
    tick++
    const t = Date.now()
    const changed: Robot[] = []
    robots.forEach((r, i) => {
      if (r.view.x !== null && r.view.status !== 'OFFLINE' && (i + tick) % 2 === 0) {
        r.view.lastSeen = t
        changed.push({ ...r.view })
      }
    })
    if (changed.length > 0) handlers.onMessage({ type: 'delta', ts: t, robots: changed })
  }, 100)

  const opened = window.setTimeout(() => {
    handlers.onOpen()
    handlers.onMessage({ type: 'snapshot', ts: Date.now(), robots: robots.map((r) => ({ ...r.view })) })
  }, 0)

  return {
    close: () => {
      window.clearTimeout(opened)
      window.clearInterval(sim)
      window.clearInterval(report)
    },
  }
}
