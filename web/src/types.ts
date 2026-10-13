// Mirrors the gateway contract (README "Gateway", ADR 0004).
// Units: metres from the south-west corner, heading in radians counter-clockwise from +x,
// battery in percent, timestamps in epoch ms.

export type RobotStatus = 'IDLE' | 'MOVING' | 'PICKING' | 'CHARGING' | 'BROKEN' | 'OFFLINE'

export const ROBOT_STATUSES: RobotStatus[] = ['MOVING', 'PICKING', 'IDLE', 'CHARGING', 'BROKEN', 'OFFLINE']

export interface Cell {
  x: number
  y: number
}

export interface Rect {
  x: number
  y: number
  width: number
  height: number
}

export interface Layout {
  width: number
  height: number
  racks: Rect[]
  chargers: Cell[]
  packStations: Cell[]
}

/** Position fields are null for a robot known only from its status. */
export interface Robot {
  id: string
  x: number | null
  y: number | null
  heading: number | null
  speed: number | null
  battery: number | null
  status: RobotStatus
  lastSeen: number | null
  statusTs: number
}

export interface FleetMessage {
  type: 'snapshot' | 'delta'
  ts: number
  robots: Robot[]
}
