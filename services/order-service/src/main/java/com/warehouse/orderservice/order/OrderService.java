package com.warehouse.orderservice.order;

import com.warehouse.common.OrderEvent;
import com.warehouse.common.OrderEventType;
import com.warehouse.common.PickJobMessage;
import com.warehouse.common.Queues;
import com.warehouse.common.Topics;
import com.warehouse.common.layout.Cell;
import com.warehouse.orderservice.catalog.SkuCatalog;
import com.warehouse.orderservice.outbox.Outbox;
import com.warehouse.orderservice.outbox.Outbox.Destination;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    /** One order line: {@code qty} units of {@code sku}. */
    public record Line(String sku, int qty) {
    }

    public static class UnknownSkuException extends RuntimeException {
        UnknownSkuException(String sku) {
            super("unknown SKU: " + sku);
        }
    }

    private final JdbcClient jdbc;
    private final Outbox outbox;
    private final SkuCatalog catalog;

    public OrderService(JdbcClient jdbc, Outbox outbox, SkuCatalog catalog) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.catalog = catalog;
    }

    /**
     * Saves the order and one pick job per line, and queues their messages in the outbox, all in
     * one transaction: either everything is stored and will be published, or nothing is.
     */
    @Transactional
    public OrderView create(List<Line> lines) {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        jdbc.sql("insert into orders (id, status, created_at, updated_at) values (:id, 'NEW', :now, :now)")
                .param("id", orderId)
                .param("now", Timestamp.from(now))
                .update();

        List<OrderView.Job> jobs = new ArrayList<>();
        for (Line line : lines) {
            Cell pick = catalog.location(line.sku()).orElseThrow(() -> new UnknownSkuException(line.sku()));
            UUID jobId = UUID.randomUUID();
            jdbc.sql("""
                    insert into pick_jobs (id, order_id, sku, qty, pick_x, pick_y, status, created_at, updated_at)
                    values (:id, :orderId, :sku, :qty, :x, :y, 'PENDING', :now, :now)
                    """)
                    .param("id", jobId)
                    .param("orderId", orderId)
                    .param("sku", line.sku())
                    .param("qty", line.qty())
                    .param("x", pick.x())
                    .param("y", pick.y())
                    .param("now", Timestamp.from(now))
                    .update();
            jobs.add(new OrderView.Job(jobId, line.sku(), line.qty(), pick, "PENDING", null, 1, null));

            // The job message's ID is the job ID, so a redelivered or re-sent job is recognisable.
            outbox.add(Destination.RABBIT, Queues.JOBS_EXCHANGE, Queues.PICK_ROUTING_KEY, jobId,
                    new PickJobMessage(jobId.toString(), orderId.toString(), line.sku(), line.qty(), pick, now.toEpochMilli()));
            UUID eventId = UUID.randomUUID();
            outbox.add(Destination.KAFKA, Topics.KAFKA_ORDER_EVENTS, orderId.toString(), eventId,
                    new OrderEvent(eventId.toString(), OrderEventType.JOB_CREATED, orderId.toString(),
                            jobId.toString(), line.sku(), null, null, now.toEpochMilli()));
        }

        UUID eventId = UUID.randomUUID();
        // Written last but relayed in ID order, so consumers see ORDER_CREATED after its JOB_CREATED events.
        outbox.add(Destination.KAFKA, Topics.KAFKA_ORDER_EVENTS, orderId.toString(), eventId,
                new OrderEvent(eventId.toString(), OrderEventType.ORDER_CREATED, orderId.toString(),
                        null, null, null, null, now.toEpochMilli()));
        return new OrderView(orderId, "NEW", now, jobs);
    }

    public Optional<OrderView> find(UUID orderId) {
        return load("select * from orders where id = :id", Map.of("id", orderId)).stream().findFirst();
    }

    /** Newest first. */
    public List<OrderView> recent(int limit) {
        return load("select * from orders order by created_at desc limit :limit", Map.of("limit", limit));
    }

    /** Runs {@code ordersSql} (a select over {@code orders}) and attaches each order's jobs. */
    private List<OrderView> load(String ordersSql, Map<String, ?> params) {
        Map<UUID, OrderView> orders = new LinkedHashMap<>();
        jdbc.sql("""
                select o.id, o.status, o.created_at,
                       j.id as job_id, j.sku, j.qty, j.pick_x, j.pick_y, j.status as job_status,
                       j.robot_id, j.attempt, j.reason
                from (%s) o
                left join pick_jobs j on j.order_id = o.id
                order by o.created_at desc, j.created_at, j.id
                """.formatted(ordersSql))
                .params(params)
                .query(rs -> {
                    UUID id = rs.getObject("id", UUID.class);
                    OrderView order = orders.get(id);
                    if (order == null) {
                        order = new OrderView(id, rs.getString("status"), rs.getTimestamp("created_at").toInstant(),
                                new ArrayList<>());
                        orders.put(id, order);
                    }
                    UUID jobId = rs.getObject("job_id", UUID.class);
                    if (jobId != null) {
                        order.jobs().add(new OrderView.Job(jobId, rs.getString("sku"), rs.getInt("qty"),
                                new Cell(rs.getInt("pick_x"), rs.getInt("pick_y")), rs.getString("job_status"),
                                rs.getString("robot_id"), rs.getInt("attempt"), rs.getString("reason")));
                    }
                });
        return List.copyOf(orders.values());
    }
}
