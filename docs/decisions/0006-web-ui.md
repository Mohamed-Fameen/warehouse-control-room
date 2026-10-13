# 0006 — Web UI: 3D fleet view

**Date:** 2026-10-14 · **Status:** accepted

## Context
Visitors watch the robot fleet live in the browser. Robots report at 5 Hz and the gateway batches at 10 Hz (ADR 0004), so raw positions arrive in steps. The view has to look smooth, stay cheap with dozens of robots, and be workable while the backend is down.

## Decisions
- **Vite + React + TypeScript, React Three Fiber and drei** for the 3D scene, **zustand** for state. One store holds the latest robot views for the HUD; React re-renders it at up to 10 Hz, which is cheap for a few dozen rows.
- **Positions do not go through React.** Each frame from the gateway is pushed into a small per-robot sample buffer (`src/state/motion.ts`). The render loop draws each robot 300 ms in the past and interpolates between the two samples around that time (heading takes the short way round). 300 ms covers one 200 ms telemetry gap plus jitter. Samples are timed by the robot's own `lastSeen`, and a smoothed offset between the browser clock and the gateway's frame `ts` maps them onto local time, so network jitter does not make robots stutter.
- **Instanced meshes** for racks, chargers, pack stations and robots (body plus a nose for heading), so the robot count barely changes the draw cost. Status is the instance colour. A robot with `x == null` (known only from its status) is listed in the panel but not drawn.
- **World mapping:** warehouse `x` → three `x`, warehouse `y` → three `-z`, up is `+y`. Heading maps straight onto the rotation about `y`.
- **The dev server proxies `/api` and `/ws` to the gateway.** The browser only ever talks to its own origin, the same shape as the public deployment behind a reverse proxy, and no gateway URL is baked into the bundle.
- **`?mock` runs a fake fleet in the browser** with the same layout and protocol (snapshot, then deltas every 100 ms, robots at 5 Hz). The UI can be built and demoed without Docker. It is opt-in, never a silent fallback, so a broken backend is visible.
- **The WebSocket reconnects with backoff** (0.5 s doubling to 8 s). The gateway sends a fresh snapshot on every connect, so nothing needs replaying.

## Consequences
- What you see is 300 ms behind the robots. Fine for watching; chaos actions in Phase 3 act on the server anyway.
- The bundle is about 1.2 MB (320 KB gzipped), mostly three.js. Worth code-splitting before the public launch.
