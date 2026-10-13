package com.warehouse.orderservice.generator;

import com.warehouse.orderservice.catalog.SkuCatalog;
import com.warehouse.orderservice.order.OrderService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Creates random orders so the warehouse has work without anyone clicking. The rate can be
 * changed at runtime ({@code PUT /api/generator}); Phase 3's "rush hour" button turns it up.
 */
@Component
public class OrderGenerator {

    private static final Logger log = LoggerFactory.getLogger(OrderGenerator.class);
    private static final long TICK_MS = 1_000;

    private final OrderService orders;
    private final SkuCatalog catalog;
    private volatile int perMinute;
    private double owed;

    public OrderGenerator(OrderService orders, SkuCatalog catalog,
                          @Value("${orders.generator.per-minute:0}") int perMinute) {
        this.orders = orders;
        this.catalog = catalog;
        this.perMinute = perMinute;
    }

    public int perMinute() {
        return perMinute;
    }

    public void setPerMinute(int perMinute) {
        this.perMinute = perMinute;
        log.info("Order generator set to {} orders/minute", perMinute);
    }

    /** Accrues fractional orders each second so low rates (say 3/min) still come out evenly. */
    @Scheduled(fixedRate = TICK_MS)
    void tick() {
        owed += perMinute * TICK_MS / 60_000.0;
        while (owed >= 1) {
            owed -= 1;
            try {
                orders.create(randomLines());
            } catch (RuntimeException e) {
                log.warn("Generated order failed: {}", e.toString());
                owed = 0; // database down: do not build up a burst
                return;
            }
        }
    }

    private List<OrderService.Line> randomLines() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int count = 1 + random.nextInt(3);
        List<OrderService.Line> lines = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            lines.add(new OrderService.Line(catalog.sku(random.nextInt(catalog.size())), 1 + random.nextInt(3)));
        }
        return lines;
    }
}
