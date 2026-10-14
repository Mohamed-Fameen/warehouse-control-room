package com.warehouse.common;

/**
 * Domain event on Kafka {@link Topics#KAFKA_ORDER_EVENTS}, keyed by {@code orderId}.
 * Written only by the order service, through its outbox, once per real state change, so one
 * order's events arrive in order and match its database rows. Delivery is at least once
 * (outbox relay), so consumers deduplicate by {@code eventId}.
 *
 * @param jobId   null for order-level events
 * @param sku     null for order-level events
 * @param robotId for {@code JOB_ASSIGNED}, {@code JOB_PICKED} and {@code JOB_COMPLETED}, otherwise null
 * @param reason  for {@code JOB_REQUEUED}, {@code JOB_FAILED} and {@code ORDER_FAILED}, otherwise null
 * @param ts      epoch ms when the change was committed
 */
public record OrderEvent(String eventId, OrderEventType type, String orderId, String jobId, String sku,
                         String robotId, String reason, long ts) {
}
