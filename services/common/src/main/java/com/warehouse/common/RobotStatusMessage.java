package com.warehouse.common;

/**
 * Retained message on {@link Topics#robotStatus}. Published on every status change; the MQTT
 * last will publishes one with {@link RobotStatus#OFFLINE}.
 *
 * @param ts epoch milliseconds of the change (0 for a last will, which is built at connect time)
 */
public record RobotStatusMessage(String robotId, RobotStatus status, long ts) {
}
