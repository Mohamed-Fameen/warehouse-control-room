import { useFleet } from '../state/fleet'
import { STATUS_COLORS } from '../scene/colors'
import type { Robot } from '../types'

function RobotRow({ robot, selected, onSelect }: { robot: Robot; selected: boolean; onSelect: () => void }) {
  const battery = robot.battery
  return (
    <li className={selected ? 'robot selected' : 'robot'} onClick={onSelect}>
      <span className="swatch" style={{ background: STATUS_COLORS[robot.status] }} />
      <span className="robot-id">{robot.id}</span>
      <span className="robot-status">{robot.status.toLowerCase()}</span>
      <span className="battery" title={battery === null ? 'battery unknown' : `${battery.toFixed(0)}%`}>
        <span
          className={battery !== null && battery < 20 ? 'battery-fill low' : 'battery-fill'}
          style={{ width: `${battery ?? 0}%` }}
        />
      </span>
    </li>
  )
}

function Details({ robot }: { robot: Robot }) {
  const fmt = (v: number | null, digits = 1, unit = '') => (v === null ? '—' : `${v.toFixed(digits)}${unit}`)
  return (
    <dl className="details">
      <dt>Position</dt>
      <dd>{robot.x === null ? 'unknown' : `${fmt(robot.x)}, ${fmt(robot.y)} m`}</dd>
      <dt>Heading</dt>
      <dd>{robot.heading === null ? '—' : `${Math.round((robot.heading * 180) / Math.PI)}°`}</dd>
      <dt>Speed</dt>
      <dd>{fmt(robot.speed, 2, ' m/s')}</dd>
      <dt>Battery</dt>
      <dd>{fmt(robot.battery, 0, '%')}</dd>
      <dt>Last seen</dt>
      <dd>{robot.lastSeen === null ? 'never' : new Date(robot.lastSeen).toLocaleTimeString()}</dd>
    </dl>
  )
}

export function FleetPanel() {
  const robots = useFleet((s) => s.robots)
  const selectedId = useFleet((s) => s.selectedId)
  const select = useFleet((s) => s.select)
  const list = Object.values(robots).sort((a, b) => a.id.localeCompare(b.id))
  const selected = selectedId ? robots[selectedId] : undefined

  return (
    <aside className="panel">
      <h2>
        Fleet <span className="muted">{list.length}</span>
      </h2>
      {selected && <Details robot={selected} />}
      {list.length === 0 ? (
        <p className="muted">No robots reporting yet.</p>
      ) : (
        <ul className="robot-list">
          {list.map((r) => (
            <RobotRow
              key={r.id}
              robot={r}
              selected={r.id === selectedId}
              onSelect={() => select(r.id === selectedId ? null : r.id)}
            />
          ))}
        </ul>
      )}
    </aside>
  )
}
