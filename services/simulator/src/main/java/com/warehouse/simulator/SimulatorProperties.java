package com.warehouse.simulator;

import com.warehouse.simulator.world.Robot.Mode;
import com.warehouse.simulator.world.RobotSpec;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Settings under {@code simulator.*}; defaults live in application.yml. */
@ConfigurationProperties("simulator")
public record SimulatorProperties(
        boolean enabled,
        Mode mode,
        String site,
        int robotCount,
        long seed,
        Duration tick,
        Duration telemetryInterval,
        Mqtt mqtt,
        Robot robot) {

    public record Mqtt(String host, int port) {
    }

    public record Robot(
            double speed,
            double drainMovingPerSec,
            double drainIdlePerSec,
            double chargePerSec,
            double lowBattery,
            double chargeTarget,
            Duration pickTime,
            Duration dropTime) {

        public RobotSpec toSpec() {
            return new RobotSpec(speed, drainMovingPerSec, drainIdlePerSec, chargePerSec,
                    lowBattery, chargeTarget, pickTime.toMillis() / 1000.0, dropTime.toMillis() / 1000.0);
        }
    }
}
