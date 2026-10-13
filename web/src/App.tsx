import { useEffect, useState } from 'react'
import { connectFleet, fetchLayout } from './net/fleetSocket'
import { connectMockFleet, mockLayout } from './net/mockFleet'
import { Scene } from './scene/Scene'
import { useFleet } from './state/fleet'
import type { Layout } from './types'
import { FleetPanel } from './ui/FleetPanel'
import { TopBar } from './ui/TopBar'

const mock = new URLSearchParams(location.search).has('mock')

function useLayout(): Layout | null {
  const [layout, setLayout] = useState<Layout | null>(mock ? mockLayout() : null)
  useEffect(() => {
    if (mock) return
    const abort = new AbortController()
    let timer: number | undefined
    const load = () =>
      fetchLayout(abort.signal).then(setLayout, () => {
        if (!abort.signal.aborted) timer = window.setTimeout(load, 2000)
      })
    load()
    return () => {
      abort.abort()
      window.clearTimeout(timer)
    }
  }, [])
  return layout
}

function useFleetFeed() {
  useEffect(() => {
    const { apply, setConnection } = useFleet.getState()
    const handlers = {
      onMessage: apply,
      onOpen: () => setConnection('live'),
      onClose: () => setConnection('reconnecting'),
    }
    const feed = mock ? connectMockFleet(handlers) : connectFleet(handlers)
    return feed.close
  }, [])
}

export default function App() {
  const layout = useLayout()
  useFleetFeed()
  return (
    <div className="app">
      <TopBar mock={mock} />
      <main className="stage">
        {layout ? (
          <Scene layout={layout} />
        ) : (
          <div className="loading">
            <p>
              Waiting for the gateway at <code>/api/layout</code>…
              <br />
              Start it, or open <a href="?mock">?mock</a> to use simulated data.
            </p>
          </div>
        )}
        <FleetPanel />
      </main>
    </div>
  )
}
