package com.warehouse.common;

/**
 * MQTT topic templates and Kafka topic names shared by all services.
 */
public final class Topics {

    private Topics() {
    }

    /** MQTT single-level wildcard; pass as {@code site} to match every site. */
    public static final String ANY_SITE = "+";

    /** MQTT: position, battery, status. QoS 0. */
    public static String robotTelemetry(String site, String robotId) {
        return "wh/%s/robot/%s/telemetry".formatted(site, robotId);
    }

    /** MQTT: retained; the last will sets {@code offline}. */
    public static String robotStatus(String site, String robotId) {
        return "wh/%s/robot/%s/status".formatted(site, robotId);
    }

    /** MQTT: commands from dispatchers. QoS 1. */
    public static String robotCommand(String site, String robotId) {
        return "wh/%s/robot/%s/cmd".formatted(site, robotId);
    }

    /** MQTT wildcard for every robot's telemetry at a site. */
    public static String allRobotTelemetry(String site) {
        return "wh/%s/robot/+/telemetry".formatted(site);
    }

    /** MQTT wildcard for every robot's status at a site. */
    public static String allRobotStatus(String site) {
        return "wh/%s/robot/+/status".formatted(site);
    }

    /** Kafka: robot telemetry, keyed by robot ID. */
    public static final String KAFKA_ROBOT_TELEMETRY = "robot.telemetry";

    /** Kafka: robot status changes, keyed by robot ID. Compacted: keeps the latest status per robot. */
    public static final String KAFKA_ROBOT_STATUS = "robot.status";
}
