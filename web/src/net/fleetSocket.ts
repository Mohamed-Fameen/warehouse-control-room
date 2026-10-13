import type { FleetMessage, Layout } from '../types'

export interface FleetHandlers {
  onMessage: (msg: FleetMessage) => void
  onOpen: () => void
  onClose: () => void
}

export interface FleetFeed {
  close: () => void
}

export async function fetchLayout(signal?: AbortSignal): Promise<Layout> {
  const res = await fetch('/api/layout', { signal })
  if (!res.ok) throw new Error(`layout: HTTP ${res.status}`)
  return res.json()
}

/**
 * Connects to /ws/fleet and reconnects with backoff. The gateway sends a fresh snapshot on
 * every connect, so nothing needs replaying after a drop.
 */
export function connectFleet(handlers: FleetHandlers): FleetFeed {
  const url = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/fleet`
  let ws: WebSocket | null = null
  let retryMs = 500
  let timer: number | undefined
  let closed = false

  const open = () => {
    ws = new WebSocket(url)
    ws.onopen = () => {
      retryMs = 500
      handlers.onOpen()
    }
    ws.onmessage = (e) => handlers.onMessage(JSON.parse(e.data))
    ws.onclose = () => {
      if (closed) return
      handlers.onClose()
      timer = window.setTimeout(open, retryMs)
      retryMs = Math.min(retryMs * 2, 8000)
    }
  }
  open()

  return {
    close: () => {
      closed = true
      window.clearTimeout(timer)
      ws?.close()
    },
  }
}
