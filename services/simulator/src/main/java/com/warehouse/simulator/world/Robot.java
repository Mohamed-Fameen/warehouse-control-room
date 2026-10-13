package com.warehouse.simulator.world;

import com.warehouse.common.RobotJobState;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.layout.Cell;
import com.warehouse.common.layout.WarehouseGrid;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.random.RandomGenerator;

/**
 * One simulated robot.
 *
 * <ul>
 *   <li>{@link Mode#AUTONOMOUS}: runs its own loop: drive to a random shelf, pick, drive to a
 *       pack station, drop, repeat, and charge when low.</li>
 *   <li>{@link Mode#DISPATCHED}: waits for {@link #assign} from a dispatcher, reports progress
 *       through {@link #drainJobEvents()}, and parks on a charger between jobs.</li>
 * </ul>
 *
 * Not thread-safe: each robot is stepped by a single thread.
 */
public final class Robot {

    public enum Mode { AUTONOMOUS, DISPATCHED }

    /** A change in this robot's job, to be published by the caller. */
    public record JobEvent(String jobId, RobotJobState state, String reason) {
    }

    private record Job(String id, Cell pick) {
    }

    private enum Task { IDLE, TO_SHELF, PICKING, TO_PACK, DROPPING, TO_CHARGER, CHARGING, DEAD }

    /** A dispatched robot this long idle away from a charger goes to park on one. */
    private static final double PARK_AFTER_SECONDS = 3;
    /** How often an idle dispatched robot looks for a charger again when all are taken. */
    private static final double RETRY_SECONDS = 5;
    /** A parked robot tops up again once its battery is this far below the charge target. */
    private static final double TOP_UP_GAP = 10;

    private final String id;
    private final WarehouseGrid grid;
    private final ChargerPool chargers;
    private final RobotSpec spec;
    private final RandomGenerator random;
    private final Mode mode;

    private double x;
    private double y;
    private double heading;
    private double speed;
    private double battery;
    private Task task = Task.IDLE;
    private double waitSeconds;
    private final Deque<Cell> path = new ArrayDeque<>();
    private Cell charger;
    private Job job;
    private RobotJobState jobState;
    private final List<JobEvent> jobEvents = new ArrayList<>();

    /**
     * @param dock a charger cell the caller has already reserved for this robot; it starts there
     */
    public Robot(String id, Cell dock, double battery, WarehouseGrid grid, ChargerPool chargers,
                 RobotSpec spec, RandomGenerator random, Mode mode) {
        this.id = id;
        this.grid = grid;
        this.chargers = chargers;
        this.spec = spec;
        this.random = random;
        this.mode = mode == null ? Mode.AUTONOMOUS : mode;
        this.x = dock.centerX();
        this.y = dock.centerY();
        this.heading = Math.PI / 2;
        this.battery = battery;
        this.charger = dock;
        this.waitSeconds = random.nextDouble() * 3; // stagger departures
    }

    /**
     * Offers a job. Emits {@code ACCEPTED} or {@code REJECTED} through {@link #drainJobEvents()}.
     * Offering the job the robot already holds (a repeated QoS 1 command) repeats its current state.
     */
    public void assign(String jobId, Cell pick) {
        if (job != null && job.id().equals(jobId)) {
            jobEvents.add(new JobEvent(jobId, jobState, null));
            return;
        }
        String refusal = refusal(pick);
        if (refusal != null) {
            jobEvents.add(new JobEvent(jobId, RobotJobState.REJECTED, refusal));
            return;
        }
        if (charger != null) {
            chargers.release(charger);
            charger = null;
        }
        job = new Job(jobId, pick);
        setJobState(RobotJobState.ACCEPTED, null);
        goTo(pick, Task.TO_SHELF);
    }

    private String refusal(Cell pick) {
        if (mode != Mode.DISPATCHED) {
            return "autonomous";
        }
        if (task == Task.DEAD) {
            return "broken";
        }
        if (job != null || !(task == Task.IDLE || task == Task.CHARGING || task == Task.TO_CHARGER)) {
            return "busy";
        }
        if (battery <= spec.lowBattery()) {
            return "low battery";
        }
        if (!grid.pickSpots().contains(pick)) {
            return "not a pick spot: " + pick;
        }
        return null;
    }

    /** Advances the simulation by {@code dt} seconds. */
    public void step(double dt) {
        switch (task) {
            case IDLE -> {
                drain(spec.drainIdlePerSec() * dt);
                waitSeconds -= dt;
                if (waitSeconds <= 0 && task == Task.IDLE) {
                    if (mode == Mode.AUTONOMOUS) {
                        chooseNextTask();
                    } else {
                        idleBetweenJobs();
                    }
                }
            }
            case TO_SHELF, TO_PACK, TO_CHARGER -> {
                move(dt);
                drain(spec.drainMovingPerSec() * dt);
                if (path.isEmpty() && task != Task.DEAD) {
                    arrive();
                }
            }
            case PICKING, DROPPING -> {
                drain(spec.drainIdlePerSec() * dt);
                waitSeconds -= dt;
                if (waitSeconds <= 0 && task != Task.DEAD) {
                    if (task == Task.PICKING) {
                        finishPick();
                    } else {
                        finishDrop();
                    }
                }
            }
            case CHARGING -> {
                battery = Math.min(100, battery + spec.chargePerSec() * dt);
                if (battery >= spec.chargeTarget()) {
                    rest(0.5 + random.nextDouble());
                }
            }
            case DEAD -> {
            }
        }
    }

    private void finishPick() {
        if (job != null) {
            setJobState(RobotJobState.PICKED, null);
            goTo(nearestPackStation(), Task.TO_PACK);
        } else {
            goTo(randomOf(grid.packStations()), Task.TO_PACK);
        }
    }

    private void finishDrop() {
        if (job != null) {
            setJobState(RobotJobState.COMPLETED, null);
            job = null;
            rest(PARK_AFTER_SECONDS);
        } else {
            rest(0.5 + random.nextDouble() * 2.5);
        }
    }

    private void chooseNextTask() {
        Cell here = currentCell();
        if (battery <= spec.lowBattery()) {
            if (charger == null) {
                charger = chargers.reserveNearest(here).orElse(null);
            }
            if (charger != null) {
                goTo(charger, Task.TO_CHARGER);
                return;
            }
            // Every charger is busy: keep working and try again after the next job.
        }
        if (charger != null) {
            chargers.release(charger);
            charger = null;
        }
        goTo(randomOf(grid.pickSpots()), Task.TO_SHELF);
    }

    /** Dispatched and without a job: top up if parked, otherwise go and park on a charger. */
    private void idleBetweenJobs() {
        if (charger != null) { // parked: we only hold a charger while on it or driving to it
            if (battery < spec.chargeTarget() - TOP_UP_GAP) {
                task = Task.CHARGING;
            } else {
                waitSeconds = RETRY_SECONDS;
            }
            return;
        }
        charger = chargers.reserveNearest(currentCell()).orElse(null);
        if (charger != null) {
            goTo(charger, Task.TO_CHARGER);
        } else {
            waitSeconds = RETRY_SECONDS;
        }
    }

    private void arrive() {
        speed = 0;
        switch (task) {
            case TO_SHELF -> {
                task = Task.PICKING;
                waitSeconds = spec.pickSeconds();
                faceShelf();
            }
            case TO_PACK -> {
                task = Task.DROPPING;
                waitSeconds = spec.dropSeconds();
                heading = Math.PI / 2;
            }
            case TO_CHARGER -> task = Task.CHARGING;
            default -> throw new IllegalStateException("arrive() in " + task);
        }
    }

    private void goTo(Cell target, Task movingTask) {
        path.clear();
        path.addAll(grid.path(currentCell(), target));
        task = movingTask;
        if (path.isEmpty()) {
            arrive();
        }
    }

    private void rest(double seconds) {
        task = Task.IDLE;
        waitSeconds = seconds;
        speed = 0;
    }

    /** Moves along the path, carrying leftover distance past each waypoint. */
    private void move(double dt) {
        double budget = spec.speed() * dt;
        while (budget > 0 && !path.isEmpty()) {
            Cell next = path.peek();
            double dx = next.centerX() - x;
            double dy = next.centerY() - y;
            double distance = Math.hypot(dx, dy);
            if (distance > 1e-9) {
                heading = Math.atan2(dy, dx);
            }
            if (distance <= budget) {
                x = next.centerX();
                y = next.centerY();
                budget -= distance;
                path.poll();
            } else {
                x += dx / distance * budget;
                y += dy / distance * budget;
                budget = 0;
            }
        }
        speed = path.isEmpty() ? 0 : spec.speed();
    }

    private void drain(double amount) {
        battery = Math.max(0, battery - amount);
        if (battery == 0 && task != Task.DEAD) {
            task = Task.DEAD;
            path.clear();
            speed = 0;
            if (job != null) {
                setJobState(RobotJobState.FAILED, "battery empty");
                job = null;
            }
        }
    }

    private void setJobState(RobotJobState state, String reason) {
        jobState = state;
        jobEvents.add(new JobEvent(job.id(), state, reason));
    }

    private void faceShelf() {
        Cell here = currentCell();
        heading = grid.isShelf(here.x() + 1, here.y()) ? 0 : Math.PI;
    }

    private Cell nearestPackStation() {
        Cell here = currentCell();
        return grid.packStations().stream().min(Comparator.comparingInt(here::manhattan)).orElseThrow();
    }

    private Cell randomOf(List<Cell> cells) {
        return cells.get(random.nextInt(cells.size()));
    }

    /** Job changes since the last call, oldest first. */
    public List<JobEvent> drainJobEvents() {
        if (jobEvents.isEmpty()) {
            return List.of();
        }
        List<JobEvent> events = List.copyOf(jobEvents);
        jobEvents.clear();
        return events;
    }

    /** The job this robot is working on, with its state, if any. */
    public Optional<JobEvent> currentJob() {
        return job == null ? Optional.empty() : Optional.of(new JobEvent(job.id(), jobState, null));
    }

    public Cell currentCell() {
        return new Cell((int) Math.floor(x), (int) Math.floor(y));
    }

    public RobotStatus status() {
        return switch (task) {
            case IDLE -> RobotStatus.IDLE;
            case TO_SHELF, TO_PACK, TO_CHARGER -> RobotStatus.MOVING;
            case PICKING, DROPPING -> RobotStatus.PICKING;
            case CHARGING -> RobotStatus.CHARGING;
            case DEAD -> RobotStatus.BROKEN;
        };
    }

    public String id() {
        return id;
    }

    public double x() {
        return x;
    }

    public double y() {
        return y;
    }

    public double heading() {
        return heading;
    }

    public double speed() {
        return speed;
    }

    public double battery() {
        return battery;
    }

    /** The charger this robot holds, if any. */
    public Optional<Cell> charger() {
        return Optional.ofNullable(charger);
    }
}
