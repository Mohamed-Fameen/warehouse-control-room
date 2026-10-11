package com.warehouse.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TopicsTest {

    @Test
    void buildsMqttTopics() {
        assertThat(Topics.robotTelemetry("dc1", "r-7")).isEqualTo("wh/dc1/robot/r-7/telemetry");
        assertThat(Topics.robotStatus("dc1", "r-7")).isEqualTo("wh/dc1/robot/r-7/status");
        assertThat(Topics.robotCommand("dc1", "r-7")).isEqualTo("wh/dc1/robot/r-7/cmd");
        assertThat(Topics.allRobotTelemetry("dc1")).isEqualTo("wh/dc1/robot/+/telemetry");
        assertThat(Topics.allRobotStatus(Topics.ANY_SITE)).isEqualTo("wh/+/robot/+/status");
    }
}
