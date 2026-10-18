package com.warehouse.common;

/**
 * A fact from a dispatcher about one pick job, on Kafka {@link Topics#KAFKA_JOB_EVENTS}, keyed by
 * {@code orderId} so one order's events stay in order. At least once: consumers must tolerate
 * duplicates (for example a {@code JOB_COMPLETED} re-sent after a dispatcher restart).
 *
 * @param robotId null when no robot is involved
 * @param reason  for {@code JOB_REQUEUED} and {@code JOB_DEAD_LETTERED}, otherwise null
 * @param attempt 1 for the first delivery of the job, 2 after one requeue, and so on
 * @param ts      epoch ms
 */
public record JobEvent(String eventId, JobEventType type, String jobId, String orderId, String robotId,
                       String reason, int attempt, long ts) {
}
