# 0006 — Dispatcher: pick jobs to robots

**Date:** 2026-10-14 · **Status:** accepted

## Context
Phase 2a puts pick jobs on the RabbitMQ quorum queue `pick-jobs`. Something must give each job to one robot, notice when a robot fails, and keep working when a dispatcher replica dies, without losing jobs and with as few duplicate picks as possible.

## Decisions
- **Robots get a fourth MQTT topic, `wh/{site}/robot/{id}/job`** (`RobotJobUpdate`, QoS 1). The robot reports `ACCEPTED`, `PICKED`, `COMPLETED`, `FAILED` (all **retained**) and `REJECTED` (not retained, so it never replaces the robot's real job in the retained slot). A robot with no job clears the retained message (empty payload) on every connect, so a restarted robot never claims a job it has lost. Commands go the other way on `.../cmd` (`RobotCommand`, QoS 1, not retained, so a robot that reconnects later does not replay old commands).
- **A job stays unacked on RabbitMQ until its robot reports `COMPLETED`.** Failures need no retry code of our own:
  - A robot fails, breaks, goes offline, clears its job, or the job runs past 3 minutes → `basic.nack` with requeue. RabbitMQ delivers it again, to this or another dispatcher, and after 5 deliveries the quorum queue dead-letters it to `pick-jobs.dlq`.
  - A dispatcher dies → its channel closes and RabbitMQ requeues every job it held.
  - The listener prefetch (default 40) caps how many jobs one replica holds, so other replicas get the rest.
  - Quorum queues have a consumer timeout (30 min by default); jobs finish far sooner.
- **Taking over a redelivered job.** The dispatcher subscribes to every robot's retained `job` topic. When a redelivered job is already `ACCEPTED`/`PICKED` on some robot, it adopts that run instead of sending the job out again. It also remembers completed job IDs for 10 minutes and acks redeliveries of finished jobs straight away. Two rules came out of chaos testing:
  - **No assignments until the fleet is known**: MQTT subscribed + 2 s for the retained messages. Without it, a restarted dispatcher received its redelivered jobs before the robots' retained job states and sent running jobs to a second robot.
  - **Never adopt from a robot that is `OFFLINE` or `BROKEN`.** Its retained state still says `ACCEPTED`, but that run has stopped. Adopting from it left jobs stuck until the 3-minute timeout.

  A duplicate pick can still happen if a robot finished the job *and* moved on to another one before the redelivery arrives; delivery stays at least once.
- **Robots arbitrate between replicas.** Each dispatcher sees the whole fleet (normal MQTT subscriptions, not shared), so two replicas may pick the same idle robot. The robot accepts the first job and rejects the rest; the losing dispatcher puts its job back to pending and skips that robot for 5 s. No leader election or robot partitioning is needed.
- **Assignment: FIFO jobs, nearest free robot (Manhattan distance).** A robot is free when it is `IDLE` or `CHARGING`, has at least 45 % battery (robots refuse at or below 40 %), has sent telemetry in the last 5 s and has no active job. No answer within 5 s → try another robot. Simple on purpose: Phase 8 replaces this with an RL policy.
- **One thread owns the dispatcher state.** `JobBoard` is plain logic with no I/O and is unit-tested without brokers. RabbitMQ deliveries, MQTT messages and a 100 ms tick are all applied on one loop thread, so no locks are needed. Acks happen on that thread, off the listener thread: Spring AMQP allows deferred manual acks, and an ack on a channel that has since closed is dropped (the broker has already requeued the message).
- **Simulator modes.** `SIM_MODE=dispatched` (compose default) makes robots wait for commands and park on the nearest free charger between jobs, topping up below 85 %. `autonomous` (default when the simulator runs on its own) keeps the Phase 1 random loop, so the UI can be developed without the order service and dispatcher.
- **Unreadable job messages are rejected without requeue**, so they go straight to the DLQ.
