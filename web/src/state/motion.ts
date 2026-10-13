import type { Robot } from '../types'

// Robots report at 5 Hz and the gateway batches at 10 Hz, so positions arrive in steps.
// We keep a short buffer of samples per robot and draw each one slightly in the past,
// interpolating between the two samples around the render time.

/** How far behind the newest data we draw. Covers one 200 ms telemetry gap plus jitter. */
const RENDER_DELAY_MS = 300
const MAX_SAMPLES = 16

interface Sample {
  t: number // server epoch ms (robot timestamp)
  x: number
  y: number
  heading: number
}

export interface Pose {
  x: number
  y: number
  heading: number
}

const tracks = new Map<string, Sample[]>()

/** Local clock minus server clock, smoothed. */
let clockOffset: number | null = null

export function noteServerTime(serverTs: number) {
  const offset = Date.now() - serverTs
  clockOffset = clockOffset === null ? offset : clockOffset + 0.05 * (offset - clockOffset)
}

export function pushRobot(robot: Robot) {
  if (robot.x === null || robot.y === null || robot.lastSeen === null) {
    tracks.delete(robot.id)
    return
  }
  let track = tracks.get(robot.id)
  if (!track) {
    track = []
    tracks.set(robot.id, track)
  }
  const last = track[track.length - 1]
  if (last && robot.lastSeen <= last.t) return
  track.push({ t: robot.lastSeen, x: robot.x, y: robot.y, heading: robot.heading ?? 0 })
  if (track.length > MAX_SAMPLES) track.splice(0, track.length - MAX_SAMPLES)
}

export function resetTracks(keep: Set<string>) {
  for (const id of tracks.keys()) if (!keep.has(id)) tracks.delete(id)
}

export function renderTime(): number {
  return Date.now() - (clockOffset ?? 0) - RENDER_DELAY_MS
}

/** Interpolated pose at server time {@code t}, or null if the robot has no position. */
export function poseAt(id: string, t: number, out: Pose): Pose | null {
  const track = tracks.get(id)
  if (!track || track.length === 0) return null
  if (t <= track[0].t) return copy(track[0], out)
  const last = track[track.length - 1]
  if (t >= last.t) return copy(last, out)
  let i = track.length - 1
  while (track[i - 1].t > t) i--
  const a = track[i - 1]
  const b = track[i]
  const k = (t - a.t) / (b.t - a.t)
  out.x = a.x + (b.x - a.x) * k
  out.y = a.y + (b.y - a.y) * k
  out.heading = a.heading + shortestAngle(b.heading - a.heading) * k
  return out
}

function copy(s: Sample, out: Pose): Pose {
  out.x = s.x
  out.y = s.y
  out.heading = s.heading
  return out
}

function shortestAngle(d: number): number {
  return Math.atan2(Math.sin(d), Math.cos(d))
}
