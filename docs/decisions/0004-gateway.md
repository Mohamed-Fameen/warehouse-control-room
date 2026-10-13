# 0004 — Gateway: Kafka to browser

**Date:** 2026-10-13 · **Status:** accepted

## Context
The browser needs the live state of every robot, smoothly, without being flooded: 20 robots at 5 Hz is 100 updates a second, and Phase 4 makes this public. The UI also needs the floor plan to draw.

## Decisions
- **Plain `KafkaConsumer`, no consumer group.** Every gateway replica serves its own browsers, so each needs every robot. That is a broadcast, the opposite of the bridge's shared subscription, which splits work. The consumer assigns itself all partitions and commits nothing.
  - `robot.status` is read **from the beginning**. It is compacted, so this replays the latest status per robot and a restarted gateway knows which robots are offline.
  - `robot.telemetry` is read **from the end**. Old positions are useless, and replaying hours of telemetry would delay startup.
  - Unparseable records are counted (`gateway.records{outcome=unparseable}`) and skipped; one bad record must not stop the live view.
- **Merge rule.** Telemetry replaces the whole robot view, including status. Status messages change only the status. Kafka gives no order between the two topics, so telemetry with `ts <=` the time of an `OFFLINE` status is ignored. Otherwise a sample in flight during a clean shutdown could revive a stopped robot. (A last will has `ts` 0, so any newer telemetry, such as from a robot that reconnected, brings it back.)
- **Robots known only from `robot.status` have null position fields.** After a gateway restart, offline robots appear with status but no position. The UI must handle `x == null`.
- **WebSocket `/ws/fleet`, server to browser only.** It sends a `snapshot` (all robots) on connect, then a `delta` every 100 ms holding only the robots that changed. Each robot in a delta is complete, not a field diff, so a repeated or dropped frame does no lasting harm, and the client logic is a plain upsert by `id`. No frame is sent when nothing changed.
- **Join and tick share a lock.** A new session gets its snapshot and joins the broadcast list atomically with respect to frame building, so no change can fall between its snapshot and its first delta.
- **Slow browsers are disconnected, not buffered forever.** Each session is wrapped in `ConcurrentWebSocketSessionDecorator` (2 s send limit, 512 KB buffer). A client that falls behind is closed, reconnects and gets a fresh snapshot. Dropping deltas instead would leave idle robots stale forever, because they are never resent.
- **Layout moved to `common`** (`com.warehouse.common.layout`: `WarehouseGrid`, `Cell`, `Rect`), so the simulator and gateway share one floor plan. The gateway serves it at `GET /api/layout`, with racks as rectangles so the UI can draw one instanced box per rack.
- **Raw WebSocket with JSON, not STOMP.** There is one stream and no client-to-server messages yet, so a topic/broker protocol adds nothing. Chaos commands in Phase 3 can go over REST.
- **Origins** come from `gateway.allowed-origins` (default: any `localhost` / `127.0.0.1` port, for the Vite dev server). This must be narrowed before the public launch.

## Measured (20 robots, all on one machine)
9.6 deltas/s, about 1.6 KB per frame, about 15 KB/s per browser. Telemetry age when it reaches the browser: p50 76 ms, p95 108 ms. That covers robot → MQTT → bridge → Kafka → gateway → WebSocket and includes up to 100 ms of frame batching.
