package com.warehouse.common;

/**
 * Periodic robot telemetry, published on {@link Topics#robotTelemetry} and forwarded to
 * Kafka {@link Topics#KAFKA_ROBOT_TELEMETRY} keyed by {@code robotId}.
 *
 * @param seq     per-robot sequence number, starts at 0 on every simulator start
 * @param ts      epoch milliseconds when the sample was taken
 * @param x       metres from the west wall
 * @param y       metres from the south wall
 * @param heading radians, counter-clockwise from +x
 * @param speed   metres per second
 * @param battery percent, 0 to 100
 */
public record RobotTelemetry(
        String robotId,
        String site,
        long seq,
        long ts,
        double x,
        double y,
        double heading,
        double speed,
        double battery,
        RobotStatus status) {
}
