import { create } from 'zustand'
import type { FleetMessage, Robot } from '../types'
import { noteServerTime, pushRobot, resetTracks } from './motion'

export type ConnectionState = 'connecting' | 'live' | 'reconnecting'

interface FleetStore {
  robots: Record<string, Robot>
  connection: ConnectionState
  lastFrameAt: number | null
  selectedId: string | null
  apply: (msg: FleetMessage) => void
  setConnection: (c: ConnectionState) => void
  select: (id: string | null) => void
}

export const useFleet = create<FleetStore>((set) => ({
  robots: {},
  connection: 'connecting',
  lastFrameAt: null,
  selectedId: null,
  apply: (msg) => {
    noteServerTime(msg.ts)
    msg.robots.forEach(pushRobot)
    set((s) => {
      const robots = msg.type === 'snapshot' ? {} : { ...s.robots }
      for (const r of msg.robots) robots[r.id] = r
      if (msg.type === 'snapshot') resetTracks(new Set(Object.keys(robots)))
      return { robots, lastFrameAt: Date.now() }
    })
  },
  setConnection: (connection) => set({ connection }),
  select: (selectedId) => set({ selectedId }),
}))
