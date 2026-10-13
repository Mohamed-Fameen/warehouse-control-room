package com.warehouse.dispatcher.board;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatcher's state: the pick jobs it holds and what it knows about every robot.
 * Pure logic with no I/O of its own, so it can be tested without brokers. Not thread-safe:
 * {@code DispatchLoop} calls it from one thread.
 *
 * <p>A job stays <b>unacknowledged</b> on RabbitMQ until its robot reports {@code COMPLETED}.
 * If the robot fails, goes offline or loses the job, the job is requeued and RabbitMQ delivers
 * it again (to this or another dispatcher); after the queue's delivery limit it is
 * dead-lettered. If this dispatcher dies, RabbitMQ redelivers everything it held.
 *
 * <pre>
 *   PENDING --command sent--> ASSIGNING --ACCEPTED--> ACTIVE --COMPLETED--> ack
 *      ^                          |                     |
 *      +--REJECTED or timeout-----+                     +--FAILED, offline, broken, timeout--> requeue
 * </pre>
 *
 * Robots arbitrate between dispatcher replicas: a robot takes one job and rejects the rest.
 */
public final class JobBoard {

    private static final Logger log = LoggerFactory.getLogger(JobBoard.class);

    /** A pick job as delivered by RabbitMQ, with a way to settle it. */
    public interface Delivery {
        PickJobMessage job();

        void ack();

        /** Return the job to the queue; it counts towards the delivery limit. */
        void requeue();

        /** False once the channel it came on has closed; the broker has then requeued it already. */
        boolean isOpen();
    }

    public interface CommandSender {
        void send(String robotId, RobotCommand command);
    }

    public record Settings(
            Duration assignTimeout,
            Duration jobTimeout,
            double minBattery,
            Duration staleAfter,
            Duration rejectCooldown,
            Duration completedMemory) {
    }

    public enum Phase { PENDING, ASSIGNING, ACTIVE }

    private static final class Entry {
        final long seq;
        Delivery delivery;
        Phase phase = Phase.PENDING;
        String robotId;
        long deadline;

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

    private final Settings settings;
    private final CommandSender sender;
    private final Map<String, Entry> jobs = new LinkedHashMap<>();
    private final Map<String, Robot> robots = new TreeMap<>();
    private final Map<String, Long> recentlyCompleted = new LinkedHashMap<>();
    private long nextSeq;
    /** No assignments before this; until MQTT has replayed the retained job states we cannot know what robots are doing. */
    private long holdUntil = Long.MAX_VALUE;

    private long assigned;
    private long completed;
    private long requeued;
    private long rejected;

    public JobBoard(Settings settings, CommandSender sender) {
        this.settings = settings;
        this.sender = sender;
    }

    // ---- inputs -------------------------------------------------------------------------

    public void onJob(Delivery delivery, long now) {
        String jobId = delivery.job().jobId();
        if (recentlyCompleted.containsKey(jobId)) {
            log.info("{} was already completed; acking the redelivery", jobId);
            delivery.ack();
            return;
        }
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
            activeOn(robotId).ifPresent(e -> requeue(e, robotId + " has no job any more"));
            return;
        }
        if (update.state() != RobotJobState.REJECTED) {
            robot.jobId = update.jobId();
            robot.jobState = update.state();
        }
        if (update.state() == RobotJobState.COMPLETED) {
            recentlyCompleted.put(update.jobId(), now);
        }
        Entry entry = jobs.get(update.jobId());
        if (entry == null) {
            return; // another dispatcher's job
        }
        switch (update.state()) {
            case ACCEPTED, PICKED -> {
                // A down robot's retained state is stale: its job comes back to us as a redelivery.
                if (robot.up() && (entry.phase != Phase.ACTIVE || !robotId.equals(entry.robotId))) {
                    activate(entry, robotId, now);
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
            case COMPLETED -> {
                jobs.remove(update.jobId());
                entry.delivery.ack();
                completed++;
            }
            case FAILED -> {
                if (robotId.equals(entry.robotId)) {
                    requeue(entry, robotId + " failed it: " + update.reason());
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

    /** Times out stale assignments, then hands pending jobs to the nearest free robots. */
    public void tick(long now) {
        for (Entry entry : List.copyOf(jobs.values())) {
            if (entry.phase == Phase.ASSIGNING && now >= entry.deadline) {
                log.info("{} did not answer for {}", entry.robotId, entry.job().jobId());
                coolDown(robot(entry.robotId), now);
                backToPending(entry);
            } else if (entry.phase == Phase.ACTIVE && now >= entry.deadline) {
                requeue(entry, "took longer than " + settings.jobTimeout());
            }
        }
        long forgetBefore = now - settings.completedMemory().toMillis();
        recentlyCompleted.values().removeIf(t -> t < forgetBefore);

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
            if (entry.robotId != null && entry.phase != Phase.PENDING) {
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
                    .filter(e -> robot.id.equals(e.robotId) && e.phase != Phase.PENDING)
                    .toList()
                    .forEach(e -> {
                        if (e.phase == Phase.ACTIVE) {
                            requeue(e, robot.id + " is " + status);
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

    private void requeue(Entry entry, String reason) {
        log.info("Requeueing {}: {}", entry.job().jobId(), reason);
        jobs.remove(entry.job().jobId());
        entry.delivery.requeue();
        requeued++;
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

    public record JobView(String jobId, String orderId, Phase phase, String robotId) {
    }

    public record RobotView(String id, RobotStatus status, double battery, String jobId, RobotJobState jobState) {
    }

    public record Snapshot(Map<Phase, Long> counts, long assigned, long completed, long requeued, long rejected,
                           List<JobView> jobs, List<RobotView> robots) {
    }

    public Snapshot snapshot() {
        Map<Phase, Long> counts = new HashMap<>();
        List<JobView> jobViews = new ArrayList<>();
        for (Entry e : jobs.values()) {
            counts.merge(e.phase, 1L, Long::sum);
            jobViews.add(new JobView(e.job().jobId(), e.job().orderId(), e.phase, e.robotId));
        }
        List<RobotView> robotViews = robots.values().stream()
                .map(r -> new RobotView(r.id, r.status, r.battery, r.jobId, r.jobState))
                .toList();
        return new Snapshot(counts, assigned, completed, requeued, rejected, jobViews, robotViews);
    }
}
