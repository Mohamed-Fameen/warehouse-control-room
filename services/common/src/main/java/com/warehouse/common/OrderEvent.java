package com.warehouse.common;

/**
 * Domain event on Kafka {@link Topics#KAFKA_ORDER_EVENTS}, keyed by {@code orderId}.
 * Delivery is at least once (outbox relay), so consumers deduplicate by {@code eventId}.
 *
 * @param jobId null for order-level events
 * @param sku   null for order-level events
 * @param ts    epoch ms when the change was committed
 */
public record OrderEvent(String eventId, OrderEventType type, String orderId, String jobId, String sku, long ts) {
}
