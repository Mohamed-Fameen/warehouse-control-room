# 0003 — MQTT to Kafka bridge

**Date:** 2026-10-11 · **Status:** accepted

## Context
Robots speak MQTT; everything downstream (gateway, ClickHouse later) reads Kafka. The bridge moves robot messages from one to the other, must be able to run as several replicas, and must not lose the "robot went offline" signal from MQTT last wills.

## Decisions
- **Forward bytes, do not parse.** The bridge maps topic to topic and key, and leaves the payload alone. Schema checks belong in consumers (and in Schema Registry later). The original MQTT topic goes in the `mqtt_topic` header.
- **Key = robot ID** (from the topic path), so all records for a robot land in one partition and stay in order.
- **Telemetry through a shared subscription** (`$share/bridge/wh/+/robot/+/telemetry`). Replicas in group `bridge` split the stream instead of each producing every message.
- **Status through a normal subscription** to `wh/+/robot/+/status`, forwarded to a **compacted** `robot.status` topic. MQTT 5 never delivers retained messages to shared subscriptions, and the retained status is how a bridge learns the current state of every robot. With several replicas each status is produced more than once, which a compacted latest-value-per-key topic tolerates. A consumer can rebuild every robot's last status by reading `robot.status` from the beginning, the same job the MQTT retained flag does.
- **Clean MQTT sessions.** We first used a persistent session (clean start off, 5 minute expiry) so status changes would queue while the bridge was down. That broke telemetry: Mosquitto kept the stopped replica's session in the share group and routed its share of QoS 0 messages to it, where they were dropped. With one live and one dead bridge, exactly half the telemetry reached Kafka. With clean sessions a stopped replica leaves the group at once. Status changes that happen while the bridge is down are not replayed one by one, but the retained status brings back the latest state on resubscribe.
- **One global publish callback**, registered once, and subscribe on every (re)connect. Clean sessions forget subscriptions, and callbacks attached per subscribe would multiply on resubscribe.
- **Never block the MQTT thread for long.** Sends are async; `max.block.ms=2000` bounds the wait when Kafka is unreachable. Telemetry is QoS 0 and may be lost in that case; status recovers through the retained message.
- **Producer:** idempotent (Kafka default), `acks=all`, lz4, `linger.ms=10`.
