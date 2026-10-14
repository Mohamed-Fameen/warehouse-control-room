package com.warehouse.orderservice.order;

import com.warehouse.common.layout.Cell;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** API shape of an order with its pick jobs. */
public record OrderView(UUID id, String status, Instant createdAt, List<Job> jobs) {

    /**
     * @param robotId robot on the current attempt, if any
     * @param reason  why the last attempt failed, or why the job was dead-lettered
     */
    public record Job(UUID id, String sku, int qty, Cell pick, String status, String robotId, int attempt, String reason) {
    }
}
