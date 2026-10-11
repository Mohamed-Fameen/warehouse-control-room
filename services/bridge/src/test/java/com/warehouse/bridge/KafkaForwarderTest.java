package com.warehouse.bridge;

import static org.assertj.core.api.Assertions.assertThat;

import com.warehouse.bridge.KafkaForwarder.Route;
import com.warehouse.common.Topics;
import org.junit.jupiter.api.Test;

class KafkaForwarderTest {

    @Test
    void routesTelemetryAndStatusByRobotId() {
        assertThat(KafkaForwarder.route(Topics.robotTelemetry("dc1", "r-07")))
                .contains(new Route(Topics.KAFKA_ROBOT_TELEMETRY, "r-07"));
        assertThat(KafkaForwarder.route(Topics.robotStatus("dc2", "r-07")))
                .contains(new Route(Topics.KAFKA_ROBOT_STATUS, "r-07"));
    }

    @Test
    void ignoresEverythingElse() {
        assertThat(KafkaForwarder.route(Topics.robotCommand("dc1", "r-07"))).isEmpty();
        assertThat(KafkaForwarder.route("wh/dc1/robot//telemetry")).isEmpty();
        assertThat(KafkaForwarder.route("wh/dc1/robot/r-07/telemetry/extra")).isEmpty();
        assertThat(KafkaForwarder.route("other/dc1/robot/r-07/telemetry")).isEmpty();
    }
}
