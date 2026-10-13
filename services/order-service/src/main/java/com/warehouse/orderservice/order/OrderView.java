package com.warehouse.orderservice.order;

import com.warehouse.common.layout.Cell;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** API shape of an order with its pick jobs. */
public record OrderView(UUID id, String status, Instant createdAt, List<Job> jobs) {

    public record Job(UUID id, String sku, int qty, Cell pick, String status) {
    }
}
