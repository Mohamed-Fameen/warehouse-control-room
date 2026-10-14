package com.warehouse.dispatcher.board;

import com.warehouse.common.JobEvent;
import com.warehouse.common.JobEventType;
import com.warehouse.common.PickJobMessage;
import com.warehouse.common.RobotCommand;
import com.warehouse.common.RobotCommandType;
import com.warehouse.common.RobotJobState;
import com.warehouse.common.RobotJobUpdate;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.common.layout.Cell;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatcher's state: the pick jobs it holds and what it knows about every robot.
 * Pure logic with no I/O of its own, so it can be tested without brokers. Not thread-safe:
 * {@code DispatchLoop} calls it from one thread, including the publish callbacks.
 *
 * <p>A job stays <b>unacknowledged</b> on RabbitMQ until it is finished. If the robot fails, goes
 * offline or loses the job, the job is requeued and RabbitMQ delivers it again (to this or another
 * dispatcher). After {@code maxAttempts} failed attempts it is rejected to the dead-letter queue.
 * If this dispatcher dies, RabbitMQ redelivers everything it held.
 *
 * <pre>
 *   PENDING --command sent--> ASSIGNING --ACCEPTED--> ACTIVE --COMPLETED--> FINISHING --event confirmed--> ack
 *      ^                          |                     |
 *      +--REJECTED or timeout-----+                     +--failed, offline, broken, timeout--> requeue
 *                                                                          (out of attempts: FINISHING --> dead-letter)
 * </pre>
 *
 * Every step is reported as a {@link JobEvent}. A job is acked or dead-lettered only after
 * Kafka has confirmed its final event, so the outcome is never lost; if the publish fails it is
 * retried on the next tick.
 *
 * <p>Robots arbitrate between dispatcher replicas: a robot takes one job and rejects the rest.
 */
public final class JobBoard {

    private static final Logger log = LoggerFactory.getLogger(JobBoard.class);

    /** A pick job as delivered by RabbitMQ, with a way to settle it. */
    public interface Delivery {
        PickJobMessage job();

        /** 1 on the first delivery, 2 after one requeue, and so on. */
        int attempt();

        void ack();

        /** Return the job to the queue; it counts towards the delivery limit. */
        void requeue();

        /** Reject without requeue: the broker moves it to the dead-letter queue. */
        void deadLetter();

        /** False once the channel it came on has closed; the broker has then requeued it already. */
        boolean isOpen();
    }

    public interface CommandSender {
        void send(String robotId, RobotCommand command);
    }

    /** Publishes job events. {@code confirmed} receives the outcome later, on the board's thread. */
    public interface EventPublisher {
        void publish(JobEvent event, Consumer<Boolean> confirmed);
    }

    public record Settings(
            Duration assignTimeout,
            Duration jobTimeout,
            double minBattery,
            Duration staleAfter,
            Duration rejectCooldown,
            Duration completedMemory,
            int maxAttempts) {
    }

    public enum Phase { PENDING, ASSIGNING, ACTIVE, FINISHING }

    private enum Outcome { ACK, DEAD_LETTER }

    private static final class Entry {
        final long seq;
        Delivery delivery;
        Phase phase = Phase.PENDING;
        String robotId;
        long deadline;
        /** In FINISHING: what to do once the final event is confirmed. */
        Outcome outcome;
        JobEvent finalEvent;
        boolean publishing;

        Entry(long seq, Delivery delivery) {
            this.seq = seq;
            this.delivery = delivery;
        }

        PickJobMessage job() {
            return delivery.job();
        }
    }

    private static final class Robot {
        final String id;
        RobotStatus status = RobotStatus.OFFLINE;
        Double x;
        Double y;
        double battery;
        long lastSeen = Long.MIN_VALUE / 2;
        long offlineTs = -1;
        String jobId;
        RobotJobState jobState;
        long cooldownUntil;

        Robot(String id) {
            this.id = id;
        }

        /** False when offline or broken: its retained job state then describes a run that has stopped. */
        boolean up() {
            return status != RobotStatus.OFFLINE && status != RobotStatus.BROKEN;
        }

        Cell cell() {
            return new Cell((int) Math.floor(x), (int) Math.floor(y));
        }
    }

    private record Completion(String robotId, long at) {
    }

    private final Settings settings;
    private final CommandSender sender;
    private final EventPublisher events;
    private final Map<String, Entry> jobs = new LinkedHashMap<>();
    private final Map<String, Robot> robots = new TreeMap<>();
    private final Map<String, Completion> recentlyCompleted = new LinkedHashMap<>();
    private long nextSeq;
    /** No assignments before this; until MQTT has replayed the retained job states we cannot know what robots are doing. */
    private long holdUntil = Long.MAX_VALUE;

    private long assigned;
    private long completed;
    private long requeued;
    private long rejected;
    private long deadLettered;

    public JobBoard(Settings settings, CommandSender sender, EventPublisher events) {
        this.settings = settings;
        this.sender = sender;
        this.events = events;
    }

    // ---- inputs -------------------------------------------------------------------------

    public void onJob(Delivery delivery, long now) {
        String jobId = delivery.job().jobId();
        Entry existing = jobs.get(jobId);
        if (existing != null) {
            if (existing.delivery.isOpen()) {
                delivery.ack(); // a duplicate publish (the outbox is at least once)
            } else {
                existing.delivery = delivery; // our channel died and the broker sent it again
            }
            return;
        }
        Entry entry = new Entry(nextSeq++, delivery);
        jobs.put(jobId, entry);
        Completion done = recentlyCompleted.get(jobId);
        if (done != null) {
            // Finished while no dispatcher held it (for example during a dispatcher restart).
            log.info("{} was already completed by {}; confirming and acking", jobId, done.robotId());
            finish(entry, Outcome.ACK, event(JobEventType.JOB_COMPLETED, entry, done.robotId(), null));
            return;
        }
        // Redelivered after a dispatcher died? A robot may already be on it.
        for (Robot robot : robots.values()) {
            if (jobId.equals(robot.jobId) && robot.jobState != null && robot.jobState.active() && robot.up()) {
                log.info("{} is already running on {}; adopting it", jobId, robot.id);
                activate(entry, robot.id, now);
                return;
            }
        }
    }

    public void onTelemetry(RobotTelemetry t, long now) {
        Robot robot = robot(t.robotId());
        if (robot.offlineTs >= 0 && t.ts() <= robot.offlineTs) {
            return; // in flight before the robot went offline
        }
        robot.offlineTs = -1;
        robot.x = t.x();
        robot.y = t.y();
        robot.battery = t.battery();
        robot.lastSeen = now;
        setStatus(robot, t.status());
    }

    public void onStatus(RobotStatusMessage s, long now) {
        Robot robot = robot(s.robotId());
        if (s.status() == RobotStatus.OFFLINE) {
            robot.offlineTs = s.ts();
        }
        setStatus(robot, s.status());
    }

    /** @param update null when the robot cleared its retained job (it has none) */
    public void onJobUpdate(String robotId, RobotJobUpdate update, long now) {
        Robot robot = robot(robotId);
        if (update == null) {
            robot.jobId = null;
            robot.jobState = null;
            activeOn(robotId).ifPresent(e -> fail(e, robotId + " has no job any more"));
            return;
        }
        if (update.state() != RobotJobState.REJECTED) {
            robot.jobId = update.jobId();
            robot.jobState = update.state();
        }
        if (update.state() == RobotJobState.COMPLETED) {
            recentlyCompleted.put(update.jobId(), new Completion(robotId, now));
        }
        Entry entry = jobs.get(update.jobId());
        if (entry == null || entry.phase == Phase.FINISHING) {
            return; // another dispatcher's job, or already done
        }
        switch (update.state()) {
            case ACCEPTED, PICKED -> {
                // A down robot's retained state is stale: its job comes back to us as a redelivery.
                if (robot.up() && (entry.phase != Phase.ACTIVE || !robotId.equals(entry.robotId))) {
                    activate(entry, robotId, now);
                    events.publish(event(JobEventType.JOB_ASSIGNED, entry, robotId, null), ok -> { });
                }
                if (update.state() == RobotJobState.PICKED && robotId.equals(entry.robotId)) {
                    events.publish(event(JobEventType.JOB_PICKED, entry, robotId, null), ok -> { });
                }
            }
            case REJECTED -> {
                if (entry.phase == Phase.ASSIGNING && robotId.equals(entry.robotId)) {
                    log.debug("{} rejected {}: {}", robotId, update.jobId(), update.reason());
                    rejected++;
                    backToPending(entry);
                    coolDown(robot, now);
                }
            }
            case COMPLETED -> finish(entry, Outcome.ACK, event(JobEventType.JOB_COMPLETED, entry, robotId, null));
            case FAILED -> {
                if (robotId.equals(entry.robotId)) {
                    fail(entry, robotId + " failed it: " + update.reason());
                }
            }
        }
    }

    /**
     * Allows assignments from {@code time} on. Called once the robot topics are subscribed, with a
     * short margin for the retained messages to arrive, so a redelivered job is adopted from the
     * robot already running it instead of being sent to a second robot.
     */
    public void holdAssignmentsUntil(long time) {
        holdUntil = time;
    }

    /** Times out stale assignments, retries unconfirmed final events, then hands pending jobs to the nearest free robots. */
    public void tick(long now) {
        for (Entry entry : List.copyOf(jobs.values())) {
            if (entry.phase == Phase.ASSIGNING && now >= entry.deadline) {
                log.info("{} did not answer for {}", entry.robotId, entry.job().jobId());
                coolDown(robot(entry.robotId), now);
                backToPending(entry);
            } else if (entry.phase == Phase.ACTIVE && now >= entry.deadline) {
                fail(entry, "took longer than " + settings.jobTimeout());
            } else if (entry.phase == Phase.FINISHING && !entry.publishing) {
                publishFinal(entry);
            }
        }
        long forgetBefore = now - settings.completedMemory().toMillis();
        recentlyCompleted.values().removeIf(c -> c.at() < forgetBefore);

        if (now < holdUntil) {
            return;
        }
        List<Robot> free = freeRobots(now);
        List<Entry> pending = jobs.values().stream()
                .filter(e -> e.phase == Phase.PENDING)
                .sorted(Comparator.comparingLong(e -> e.seq))
                .toList();
        for (Entry entry : pending) {
            if (free.isEmpty()) {
                break;
            }
            Cell pick = entry.job().pick();
            Robot best = free.stream().min(Comparator.comparingInt(r -> r.cell().manhattan(pick))).orElseThrow();
            free.remove(best);
            entry.phase = Phase.ASSIGNING;
            entry.robotId = best.id;
            entry.deadline = now + settings.assignTimeout().toMillis();
            assigned++;
            sender.send(best.id, new RobotCommand(RobotCommandType.ASSIGN_JOB, entry.job().jobId(), pick, System.currentTimeMillis()));
        }
    }

    // ---- helpers ------------------------------------------------------------------------

    private List<Robot> freeRobots(long now) {
        Set<String> taken = new HashSet<>();
        for (Entry entry : jobs.values()) {
            if (entry.phase == Phase.ASSIGNING || entry.phase == Phase.ACTIVE) {
                taken.add(entry.robotId);
            }
        }
        List<Robot> free = new ArrayList<>();
        for (Robot robot : robots.values()) {
            boolean ready = robot.status == RobotStatus.IDLE || robot.status == RobotStatus.CHARGING;
            if (ready
                    && robot.x != null
                    && robot.battery >= settings.minBattery()
                    && now - robot.lastSeen <= settings.staleAfter().toMillis()
                    && (robot.jobState == null || !robot.jobState.active())
                    && now >= robot.cooldownUntil
                    && !taken.contains(robot.id)) {
                free.add(robot);
            }
        }
        return free;
    }

    private void setStatus(Robot robot, RobotStatus status) {
        robot.status = status;
        if (status == RobotStatus.OFFLINE || status == RobotStatus.BROKEN) {
            jobs.values().stream()
                    .filter(e -> robot.id.equals(e.robotId) && (e.phase == Phase.ASSIGNING || e.phase == Phase.ACTIVE))
                    .toList()
                    .forEach(e -> {
                        if (e.phase == Phase.ACTIVE) {
                            fail(e, robot.id + " is " + status);
                        } else {
                            backToPending(e);
                        }
                    });
        }
    }

    private void activate(Entry entry, String robotId, long now) {
        entry.phase = Phase.ACTIVE;
        entry.robotId = robotId;
        entry.deadline = now + settings.jobTimeout().toMillis();
    }

    private void backToPending(Entry entry) {
        entry.phase = Phase.PENDING;
        entry.robotId = null;
    }

    /** Skip a robot that just refused or ignored a job for a while, so the job goes elsewhere. */
    private void coolDown(Robot robot, long now) {
        robot.cooldownUntil = now + settings.rejectCooldown().toMillis();
    }

    /** An attempt failed: requeue the job, or dead-letter it when it is out of attempts. */
    private void fail(Entry entry, String reason) {
        int attempt = entry.delivery.attempt();
        if (attempt >= settings.maxAttempts()) {
            log.warn("Dead-lettering {} after {} attempts: {}", entry.job().jobId(), attempt, reason);
            finish(entry, Outcome.DEAD_LETTER, event(JobEventType.JOB_DEAD_LETTERED, entry, entry.robotId,
                    "%s (attempt %d of %d)".formatted(reason, attempt, settings.maxAttempts())));
            return;
        }
        log.info("Requeueing {}: {}", entry.job().jobId(), reason);
        events.publish(event(JobEventType.JOB_REQUEUED, entry, entry.robotId, reason), ok -> { });
        jobs.remove(entry.job().jobId());
        entry.delivery.requeue();
        requeued++;
    }

    /** Publishes the final event; the job is settled on RabbitMQ only once Kafka has confirmed it. */
    private void finish(Entry entry, Outcome outcome, JobEvent finalEvent) {
        entry.phase = Phase.FINISHING;
        entry.outcome = outcome;
        entry.finalEvent = finalEvent;
        publishFinal(entry);
    }

    private void publishFinal(Entry entry) {
        entry.publishing = true;
        events.publish(entry.finalEvent, ok -> {
            entry.publishing = false;
            if (!ok) {
                log.warn("Could not publish {} for {}; retrying", entry.finalEvent.type(), entry.job().jobId());
                return; // the next tick publishes it again
            }
            if (jobs.get(entry.job().jobId()) != entry) {
                return;
            }
            jobs.remove(entry.job().jobId());
            if (entry.outcome == Outcome.ACK) {
                entry.delivery.ack();
                completed++;
            } else {
                entry.delivery.deadLetter();
                deadLettered++;
            }
        });
    }

    private JobEvent event(JobEventType type, Entry entry, String robotId, String reason) {
        PickJobMessage job = entry.job();
        return new JobEvent(UUID.randomUUID().toString(), type, job.jobId(), job.orderId(), robotId, reason,
                entry.delivery.attempt(), System.currentTimeMillis());
    }

    private Optional<Entry> activeOn(String robotId) {
        return jobs.values().stream()
                .filter(e -> e.phase == Phase.ACTIVE && robotId.equals(e.robotId))
                .findFirst();
    }

    private Robot robot(String id) {
        return robots.computeIfAbsent(id, Robot::new);
    }

    // ---- read model ---------------------------------------------------------------------

    public record JobView(String jobId, String orderId, Phase phase, String robotId, int attempt) {
    }

    public record RobotView(String id, RobotStatus status, double battery, String jobId, RobotJobState jobState) {
    }

    public record Snapshot(Map<Phase, Long> counts, long assigned, long completed, long requeued, long rejected,
                           long deadLettered, List<JobView> jobs, List<RobotView> robots) {
    }

    public Snapshot snapshot() {
        Map<Phase, Long> counts = new HashMap<>();
        List<JobView> jobViews = new ArrayList<>();
        for (Entry e : jobs.values()) {
            counts.merge(e.phase, 1L, Long::sum);
            jobViews.add(new JobView(e.job().jobId(), e.job().orderId(), e.phase, e.robotId, e.delivery.attempt()));
        }
        List<RobotView> robotViews = robots.values().stream()
                .map(r -> new RobotView(r.id, r.status, r.battery, r.jobId, r.jobState))
                .toList();
        return new Snapshot(counts, assigned, completed, requeued, rejected, deadLettered, jobViews, robotViews);
    }
}
