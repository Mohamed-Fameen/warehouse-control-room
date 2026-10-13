import type { RobotStatus } from '../types'

export const STATUS_COLORS: Record<RobotStatus, string> = {
  MOVING: '#38bdf8',
  PICKING: '#f59e0b',
  IDLE: '#94a3b8',
  CHARGING: '#22c55e',
  BROKEN: '#ef4444',
  OFFLINE: '#475569',
}
