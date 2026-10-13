import { useFleet } from '../state/fleet'
import { STATUS_COLORS } from '../scene/colors'
import { ROBOT_STATUSES, type RobotStatus } from '../types'

const CONNECTION_LABELS = {
  connecting: 'Connecting',
  live: 'Live',
  reconnecting: 'Reconnecting',
} as const

export function TopBar({ mock }: { mock: boolean }) {
  const robots = useFleet((s) => s.robots)
  const connection = useFleet((s) => s.connection)

  const counts = {} as Record<RobotStatus, number>
  for (const s of ROBOT_STATUSES) counts[s] = 0
  for (const r of Object.values(robots)) counts[r.status]++

  return (
    <header className="topbar">
      <h1>Warehouse Control Room</h1>
      <span className={`pill pill-${connection}`}>
        <span className="dot" />
        {CONNECTION_LABELS[connection]}
        {mock && ' · mock data'}
      </span>
      <ul className="counts">
        {ROBOT_STATUSES.map((s) => (
          <li key={s} title={s}>
            <span className="swatch" style={{ background: STATUS_COLORS[s] }} />
            {s.toLowerCase()} <strong>{counts[s]}</strong>
          </li>
        ))}
      </ul>
    </header>
  )
}
