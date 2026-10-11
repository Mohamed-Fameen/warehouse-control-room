# 0002 — Robot simulator

**Date:** 2026-10-11 · **Status:** accepted

## Context
Phase 1a needs a robot fleet that publishes realistic telemetry over MQTT before orders or dispatchers exist, and that later phases can break on purpose (killed robots, dead batteries).

## Decisions
- **One MQTT connection per robot**, each on its own virtual thread. Real robots each hold their own connection, and it is the only way every robot gets its own last will (`OFFLINE` on its retained status topic). Killing one robot's connection in Phase 3 then looks exactly like a robot dropping off the network.
- **HiveMQ MQTT client 1.3.6, MQTT 5, async API.** Its connections share one Netty event loop, so 20 to 37 clients cost a handful of threads; Paho v3 starts several threads per client. Automatic reconnect is on; the first connect is retried by the runner with backoff so the simulator can start before the broker.
- **Telemetry is QoS 0, dropped while disconnected.** Stale positions are worthless and the next sample arrives 200 ms later. Status is QoS 1, retained, and republished after every reconnect.
- **Clean shutdown publishes `OFFLINE` itself**, because a clean MQTT disconnect suppresses the will. The will's `ts` is 0 because it is built at connect time.
- **Grid world.** 40 x 26 m floor on a 1 m grid, two shelf bands of 2-wide racks with 2 m aisles, 37 chargers, 6 pack stations. Robots move cell to cell along BFS paths at 1.5 m/s. Positions are metres from the south-west corner; heading is radians counter-clockwise from +x.
- **Self-driven behaviour until Phase 2.** Each robot loops shelf → pick (3 s) → pack station → drop (2 s) → idle, and goes to the nearest free charger when its battery is at or below 40 % after a job (worst case a job plus the trip to a charger uses about 32 %). At 0 % it stops as `BROKEN`. Dispatchers replace the random job choice in Phase 2.
- **No collision avoidance yet.** Robots can overlap. Traffic control belongs with the dispatcher.
- **Deterministic seeds.** Robot `i` uses `SplittableRandom(seed + i)` (from `java.base`; the JRE image does not ship the `jdk.random` algorithms).
- **Payloads** are the `RobotTelemetry` and `RobotStatusMessage` records in `services/common`, as JSON.
