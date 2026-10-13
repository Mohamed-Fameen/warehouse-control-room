package com.warehouse.common;

import com.warehouse.common.layout.Cell;

/**
 * One pick job on RabbitMQ {@link Queues#PICK_JOBS}: fetch {@code qty} of {@code sku} from the
 * shelf next to {@code pick} and bring it to a pack station.
 *
 * @param pick      aisle cell to stop at; the shelf is directly east or west of it
 * @param createdAt epoch ms
 */
public record PickJobMessage(String jobId, String orderId, String sku, int qty, Cell pick, long createdAt) {
}
