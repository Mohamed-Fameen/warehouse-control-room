package com.warehouse.common;

/**
 * A robot's progress on a job, on {@link Topics#robotJob} (QoS 1).
 *
 * <p>Every state except {@link RobotJobState#REJECTED} is published <b>retained</b>, so the
 * retained message is the robot's latest job. A dispatcher that starts (or takes over a
 * redelivered job) learns from it which robot is already doing what. A robot without a job
 * clears the retained message (empty payload) when it connects.
 *
 * @param reason only for {@code REJECTED} and {@code FAILED}, otherwise null
 * @param ts     epoch ms of the change
 */
public record RobotJobUpdate(String robotId, String jobId, RobotJobState state, String reason, long ts) {
}
