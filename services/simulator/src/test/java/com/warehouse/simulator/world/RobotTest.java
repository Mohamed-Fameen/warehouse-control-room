package com.warehouse.simulator.world;

import static org.assertj.core.api.Assertions.assertThat;

import com.warehouse.common.RobotStatus;
import java.util.EnumSet;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class RobotTest {

    private static final RobotSpec SPEC = new RobotSpec(1.5, 0.3, 0.02, 2.5, 40, 95, 3, 2);
    private static final double DT = 0.1;

    private final WarehouseGrid grid = WarehouseGrid.standard();
    private final ChargerPool chargers = new ChargerPool(grid.chargers());

    private Robot robot(double battery, RobotSpec spec) {
        Cell dock = grid.chargers().getFirst();
        chargers.reserve(dock);
        RandomGenerator random = new SplittableRandom(7);
        return new Robot("r-01", dock, battery, grid, chargers, spec, random);
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
}
