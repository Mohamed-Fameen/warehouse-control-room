package com.warehouse.simulator.world;

import static org.assertj.core.api.Assertions.assertThat;

import com.warehouse.common.RobotJobState;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.layout.Cell;
import com.warehouse.common.layout.WarehouseGrid;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class RobotTest {

    private static final RobotSpec SPEC = new RobotSpec(1.5, 0.3, 0.02, 2.5, 40, 95, 3, 2);
    private static final double DT = 0.1;

    private final WarehouseGrid grid = WarehouseGrid.standard();
    private final ChargerPool chargers = new ChargerPool(grid.chargers());

    private Robot robot(double battery, RobotSpec spec) {
        return robot(battery, spec, Robot.Mode.AUTONOMOUS);
    }

    private Robot robot(double battery, RobotSpec spec, Robot.Mode mode) {
        Cell dock = grid.chargers().getFirst();
        chargers.reserve(dock);
        RandomGenerator random = new SplittableRandom(7);
        return new Robot("r-01", dock, battery, grid, chargers, spec, random, mode);
    }

    /** Steps until {@code done} or {@code seconds} pass, collecting job events. */
    private List<Robot.JobEvent> run(Robot robot, double seconds, Predicate<Robot> done) {
        List<Robot.JobEvent> events = new ArrayList<>(robot.drainJobEvents());
        for (int i = 0; i < seconds / DT && !done.test(robot); i++) {
            robot.step(DT);
            events.addAll(robot.drainJobEvents());
        }
        return events;
    }

    @Test
    void leavesDockAndCyclesThroughWork() {
        Robot robot = robot(100, SPEC);
        Set<RobotStatus> seen = EnumSet.noneOf(RobotStatus.class);
        for (int i = 0; i < 2_000; i++) { // 200 s
            robot.step(DT);
            seen.add(robot.status());
            assertThat(grid.isWalkable(robot.currentCell().x(), robot.currentCell().y())).isTrue();
            assertThat(robot.speed()).isBetween(0.0, SPEC.speed());
        }
        assertThat(seen).contains(RobotStatus.MOVING, RobotStatus.PICKING, RobotStatus.IDLE);
        assertThat(robot.charger()).isEmpty();
    }

    @Test
    void neverMovesFasterThanSpec() {
        Robot robot = robot(100, SPEC);
        double px = robot.x();
        double py = robot.y();
        for (int i = 0; i < 1_000; i++) {
            robot.step(DT);
            assertThat(Math.hypot(robot.x() - px, robot.y() - py)).isLessThanOrEqualTo(SPEC.speed() * DT + 1e-9);
            px = robot.x();
            py = robot.y();
        }
    }

    @Test
    void goesToChargeWhenLow() {
        RobotSpec hungry = new RobotSpec(1.5, 0.3, 0.02, 10, 40, 95, 3, 2);
        Robot robot = robot(41, hungry);
        boolean charged = false;
        for (int i = 0; i < 5_000 && !charged; i++) {
            robot.step(DT);
            if (robot.status() == RobotStatus.CHARGING) {
                charged = true;
                assertThat(robot.charger()).contains(robot.currentCell());
            }
        }
        assertThat(charged).isTrue();
        assertThat(robot.status()).isNotEqualTo(RobotStatus.BROKEN);
    }

    @Test
    void breaksWhenBatteryRunsOut() {
        RobotSpec leaky = new RobotSpec(1.5, 50, 50, 2.5, 0, 95, 3, 2);
        Robot robot = robot(10, leaky);
        for (int i = 0; i < 100; i++) {
            robot.step(DT);
        }
        assertThat(robot.status()).isEqualTo(RobotStatus.BROKEN);
        assertThat(robot.battery()).isZero();
        assertThat(robot.speed()).isZero();
    }

    @Test
    void dispatchedRobotWaitsOnItsDockForAJob() {
        Robot robot = robot(100, SPEC, Robot.Mode.DISPATCHED);
        Cell dock = robot.currentCell();
        run(robot, 60, r -> false);
        assertThat(robot.currentCell()).isEqualTo(dock);
        assertThat(robot.status()).isIn(RobotStatus.IDLE, RobotStatus.CHARGING);
        assertThat(robot.drainJobEvents()).isEmpty();
    }

    @Test
    void dispatchedRobotRunsAJobAndReportsEachStep() {
        Robot robot = robot(100, SPEC, Robot.Mode.DISPATCHED);
        Cell pick = grid.pickSpots().get(40);
        robot.assign("job-1", pick);
        assertThat(robot.charger()).isEmpty(); // left the dock

        List<Robot.JobEvent> events = run(robot, 300, r -> r.currentJob().isEmpty());
        assertThat(events).extracting(Robot.JobEvent::state)
                .containsExactly(RobotJobState.ACCEPTED, RobotJobState.PICKED, RobotJobState.COMPLETED);
        assertThat(events).extracting(Robot.JobEvent::jobId).containsOnly("job-1");
        assertThat(grid.packStations()).contains(robot.currentCell());

        // Then it parks on a charger.
        run(robot, 120, r -> r.status() == RobotStatus.CHARGING
                || (r.charger().isPresent() && r.charger().get().equals(r.currentCell())));
        assertThat(robot.charger()).contains(robot.currentCell());
    }

    @Test
    void repeatedCommandRepeatsStateAndOtherJobsAreRejected() {
        Robot robot = robot(100, SPEC, Robot.Mode.DISPATCHED);
        Cell pick = grid.pickSpots().get(40);
        robot.assign("job-1", pick);
        robot.assign("job-1", pick);
        robot.assign("job-2", pick);
        assertThat(robot.drainJobEvents()).containsExactly(
                new Robot.JobEvent("job-1", RobotJobState.ACCEPTED, null),
                new Robot.JobEvent("job-1", RobotJobState.ACCEPTED, null),
                new Robot.JobEvent("job-2", RobotJobState.REJECTED, "busy"));
    }

    @Test
    void rejectsJobsItCannotDo() {
        Robot autonomous = robot(100, SPEC, Robot.Mode.AUTONOMOUS);
        autonomous.assign("job-1", grid.pickSpots().getFirst());
        assertThat(autonomous.drainJobEvents()).extracting(Robot.JobEvent::reason).containsExactly("autonomous");

        Robot low = new Robot("r-02", grid.chargers().get(1), 30, grid, chargers, SPEC,
                new SplittableRandom(1), Robot.Mode.DISPATCHED);
        low.assign("job-2", grid.pickSpots().getFirst());
        assertThat(low.drainJobEvents()).extracting(Robot.JobEvent::reason).containsExactly("low battery");

        Robot robot = new Robot("r-03", grid.chargers().get(2), 100, grid, chargers, SPEC,
                new SplittableRandom(1), Robot.Mode.DISPATCHED);
        robot.assign("job-3", grid.chargers().get(2));
        assertThat(robot.drainJobEvents()).extracting(Robot.JobEvent::state).containsExactly(RobotJobState.REJECTED);
        assertThat(robot.currentJob()).isEmpty();
    }

    @Test
    void failsTheJobWhenTheBatteryRunsOut() {
        RobotSpec leaky = new RobotSpec(1.5, 5, 0.02, 2.5, 40, 95, 3, 2);
        Robot robot = robot(45, leaky, Robot.Mode.DISPATCHED);
        robot.assign("job-1", grid.pickSpots().getLast());
        List<Robot.JobEvent> events = run(robot, 60, r -> r.status() == RobotStatus.BROKEN);
        assertThat(robot.status()).isEqualTo(RobotStatus.BROKEN);
        assertThat(events).extracting(Robot.JobEvent::state).endsWith(RobotJobState.FAILED);
        assertThat(robot.currentJob()).isEmpty();
    }
}
