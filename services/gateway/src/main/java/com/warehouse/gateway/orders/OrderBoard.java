package com.warehouse.gateway.orders;

import com.warehouse.common.OrderEvent;
import com.warehouse.common.OrderEventType;
import com.warehouse.gateway.GatewayProperties;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Recent orders and their jobs, rebuilt from {@code order.events}, plus what changed since the
 * last frame. Written by the Kafka consumer thread, read by the frame broadcaster; every method
 * is synchronized.
 *
 * <p>{@code order.events} has one event per real change and keeps each order's events in order,
 * so applying them is a plain state update. Event IDs are remembered for a while because the
 * order service's outbox delivers at least once. Events for an order whose creation was before
 * the consumer's start window are ignored: the board only shows orders it saw being created.
 */
@Component
public class OrderBoard {

    /** One pick job on the board. {@code status}: PENDING, ASSIGNED, PICKED, COMPLETED or FAILED. */
    public record JobCard(String id, String sku, String status, String robotId, String reason) {
    }

    /**
     * One order on the board. {@code status}: NEW, IN_PROGRESS, COMPLETED or FAILED.
     *
     * @param createdAt epoch ms
     * @param updatedAt epoch ms of the latest event
     * @param reason    why the order failed, if it did
     */
    public record OrderCard(String id, String status, long createdAt, long updatedAt, String reason, List<JobCard> jobs) {
    }

    /** Orders that changed (each complete), and IDs of orders dropped from the board. */
    public record Changes(List<OrderCard> orders, List<String> removed) {
        public boolean isEmpty() {
            return orders.isEmpty() && removed.isEmpty();
        }
    }

    private static final int SEEN_EVENT_IDS = 10_000;

    private final int maxOrders;
    private final Map<String, OrderCard> orders = new HashMap<>();
    private final Set<String> dirty = new LinkedHashSet<>();
    private final Set<String> removed = new LinkedHashSet<>();
    private final Map<String, Boolean> seen = new LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > SEEN_EVENT_IDS;
        }
    };

    @Autowired
    public OrderBoard(GatewayProperties props) {
        this(props.maxOrders());
    }

    public OrderBoard(int maxOrders) {
        this.maxOrders = maxOrders;
    }

    public synchronized void apply(OrderEvent e) {
        if (seen.put(e.eventId(), Boolean.TRUE) != null) {
            return;
        }
        OrderCard order = orders.get(e.orderId());
        if (order == null) {
            if (e.type() != OrderEventType.JOB_CREATED && e.type() != OrderEventType.ORDER_CREATED) {
                return; // created before our window
            }
            order = new OrderCard(e.orderId(), "NEW", e.ts(), e.ts(), null, List.of());
        }
        OrderCard updated = switch (e.type()) {
            case ORDER_CREATED -> withStatus(order, "NEW", null, e.ts());
            case ORDER_STARTED -> withStatus(order, "IN_PROGRESS", null, e.ts());
            case ORDER_COMPLETED -> withStatus(order, "COMPLETED", null, e.ts());
            case ORDER_FAILED -> withStatus(order, "FAILED", e.reason(), e.ts());
            case JOB_CREATED -> withJob(order, new JobCard(e.jobId(), e.sku(), "PENDING", null, null), e.ts());
            case JOB_ASSIGNED -> updateJob(order, e, "ASSIGNED");
            case JOB_PICKED -> updateJob(order, e, "PICKED");
            case JOB_COMPLETED -> updateJob(order, e, "COMPLETED");
            case JOB_REQUEUED -> updateJob(order, e, "PENDING");
            case JOB_FAILED -> updateJob(order, e, "FAILED");
        };
        orders.put(updated.id(), updated);
        dirty.add(updated.id());
        removed.remove(updated.id());
        evict();
    }

    /** Newest first. */
    public synchronized List<OrderCard> snapshot() {
        return orders.values().stream().sorted(Comparator.comparingLong(OrderCard::createdAt).reversed()).toList();
    }

    /** Orders changed and removed since the previous call, and clears both sets. */
    public synchronized Changes drainChanges() {
        List<OrderCard> changed = new ArrayList<>();
        for (String id : dirty) {
            OrderCard order = orders.get(id);
            if (order != null) {
                changed.add(order);
            }
        }
        changed.sort(Comparator.comparingLong(OrderCard::createdAt).reversed());
        Changes changes = new Changes(changed, List.copyOf(removed));
        dirty.clear();
        removed.clear();
        return changes;
    }

    /** Drops the oldest finished orders, or the oldest of all if none is finished, to stay at {@code maxOrders}. */
    private void evict() {
        while (orders.size() > maxOrders) {
            OrderCard victim = orders.values().stream()
                    .min(Comparator.comparing((OrderCard o) -> !isFinal(o.status())).thenComparingLong(OrderCard::createdAt))
                    .orElseThrow();
            orders.remove(victim.id());
            dirty.remove(victim.id());
            removed.add(victim.id());
        }
    }

    private static boolean isFinal(String status) {
        return status.equals("COMPLETED") || status.equals("FAILED");
    }

    private static OrderCard withStatus(OrderCard o, String status, String reason, long ts) {
        return new OrderCard(o.id(), status, o.createdAt(), Math.max(o.updatedAt(), ts), reason, o.jobs());
    }

    private static OrderCard withJob(OrderCard o, JobCard job, long ts) {
        List<JobCard> jobs = new ArrayList<>(o.jobs());
        int i = 0;
        while (i < jobs.size() && !jobs.get(i).id().equals(job.id())) {
            i++;
        }
        if (i < jobs.size()) {
            jobs.set(i, job);
        } else {
            jobs.add(job);
        }
        return new OrderCard(o.id(), o.status(), o.createdAt(), Math.max(o.updatedAt(), ts), o.reason(), List.copyOf(jobs));
    }

    private static OrderCard updateJob(OrderCard o, OrderEvent e, String status) {
        JobCard old = o.jobs().stream().filter(j -> j.id().equals(e.jobId())).findFirst()
                .orElse(new JobCard(e.jobId(), e.sku(), status, null, null));
        String reason = e.reason() != null ? e.reason() : old.reason();
        return withJob(o, new JobCard(old.id(), old.sku() != null ? old.sku() : e.sku(), status, e.robotId(), reason), e.ts());
    }
}
