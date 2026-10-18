package com.warehouse.gateway.fleet;

import static org.assertj.core.api.Assertions.assertThat;

import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import org.junit.jupiter.api.Test;

class FleetStateTest {

    private final FleetState state = new FleetState();

    static RobotTelemetry telemetry(String id, long ts, double x) {
        return new RobotTelemetry(id, "dc1", ts, ts, x, 2.5, 0, 1.5, 80, RobotStatus.MOVING);
    }

    @Test
    void telemetryUpsertsAndMarksDirtyOnce() {
        state.apply(telemetry("r-02", 1_000, 1));
        state.apply(telemetry("r-01", 1_000, 1));
        state.apply(telemetry("r-01", 1_200, 2));

        assertThat(state.drainChanges()).extracting(RobotView::id).containsExactly("r-01", "r-02");
        assertThat(state.drainChanges()).isEmpty();
        assertThat(state.snapshot()).hasSize(2).first().satisfies(r -> assertThat(r.x()).isEqualTo(2.0));
    }

    @Test
    void statusOnlyRobotHasNoPosition() {
        state.apply(new RobotStatusMessage("r-05", RobotStatus.OFFLINE, 0));

        RobotView view = state.snapshot().getFirst();
        assertThat(view.status()).isEqualTo(RobotStatus.OFFLINE);
        assertThat(view.x()).isNull();
        assertThat(view.lastSeen()).isNull();
    }

    @Test
    void statusKeepsLastPosition() {
        state.apply(telemetry("r-01", 1_000, 7));
        state.apply(new RobotStatusMessage("r-01", RobotStatus.OFFLINE, 2_000));

        RobotView view = state.snapshot().getFirst();
        assertThat(view.status()).isEqualTo(RobotStatus.OFFLINE);
        assertThat(view.x()).isEqualTo(7.0);
    }

    @Test
    void lateTelemetryDoesNotReviveOfflineRobot() {
        state.apply(new RobotStatusMessage("r-01", RobotStatus.OFFLINE, 2_000));
        state.drainChanges();

        state.apply(telemetry("r-01", 1_900, 3)); // sampled before the robot went offline
        assertThat(state.snapshot().getFirst().status()).isEqualTo(RobotStatus.OFFLINE);
        assertThat(state.drainChanges()).isEmpty();

        state.apply(telemetry("r-01", 2_100, 3)); // robot is back
        assertThat(state.snapshot().getFirst().status()).isEqualTo(RobotStatus.MOVING);
    }
}
