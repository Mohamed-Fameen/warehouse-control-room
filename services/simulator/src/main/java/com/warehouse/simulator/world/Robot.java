package com.warehouse.simulator.world;

import com.warehouse.common.RobotStatus;
import com.warehouse.common.layout.Cell;
import com.warehouse.common.layout.WarehouseGrid;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.random.RandomGenerator;

/**
 * One simulated robot. Until dispatchers exist (Phase 2) it runs its own loop:
 * drive to a random shelf, pick, drive to a pack station, drop, repeat, and charge when low.
 * Not thread-safe: each robot is stepped by a single thread.
 */
public final class Robot {

    private enum Task { IDLE, TO_SHELF, PICKING, TO_PACK, DROPPING, TO_CHARGER, CHARGING, DEAD }

    private final String id;
    private final WarehouseGrid grid;
    private final ChargerPool chargers;
    private final RobotSpec spec;
    private final RandomGenerator random;

    private double x;
    private double y;
    private double heading;
    private double speed;
    private double battery;
    private Task task = Task.IDLE;
    private double waitSeconds;
    private final Deque<Cell> path = new ArrayDeque<>();
    private Cell charger;

    /**
     * @param dock a charger cell the caller has already reserved for this robot; it starts there
     */
    public Robot(String id, Cell dock, double battery, WarehouseGrid grid, ChargerPool chargers,
                 RobotSpec spec, RandomGenerator random) {
        this.id = id;
        this.grid = grid;
        this.chargers = chargers;
        this.spec = spec;
        this.random = random;
        this.x = dock.centerX();
        this.y = dock.centerY();
        this.heading = Math.PI / 2;
        this.battery = battery;
        this.charger = dock;
        this.waitSeconds = random.nextDouble() * 3; // stagger departures
    }

    /** Advances the simulation by {@code dt} seconds. */
    public void step(double dt) {
        switch (task) {
            case IDLE -> {
                drain(spec.drainIdlePerSec() * dt);
                waitSeconds -= dt;
                if (waitSeconds <= 0) {
                    chooseNextTask();
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
                if (waitSeconds <= 0) {
                    if (task == Task.PICKING) {
                        goTo(randomOf(grid.packStations()), Task.TO_PACK);
                    } else {
                        rest(0.5 + random.nextDouble() * 2.5);
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
        if (battery == 0) {
            task = Task.DEAD;
            path.clear();
            speed = 0;
        }
    }

    private void faceShelf() {
        Cell here = currentCell();
        heading = grid.isShelf(here.x() + 1, here.y()) ? 0 : Math.PI;
    }

    private Cell randomOf(List<Cell> cells) {
        return cells.get(random.nextInt(cells.size()));
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
