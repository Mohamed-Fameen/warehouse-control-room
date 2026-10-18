package com.warehouse.orderservice.progress;

import com.warehouse.common.JobEvent;
import com.warehouse.common.OrderEvent;
import com.warehouse.common.OrderEventType;
import com.warehouse.common.Topics;
import com.warehouse.orderservice.outbox.Outbox;
import com.warehouse.orderservice.outbox.Outbox.Destination;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies dispatcher {@link JobEvent}s to the job and its order, and records each real change as
 * an {@link OrderEvent} in the outbox, in the same transaction.
 *
 * <p>Job events are at least once and come from several dispatchers, so every rule here is
 * idempotent: a finished job never changes again, an event from an older attempt than the one
 * recorded is ignored, and an event that changes nothing writes nothing. That turns the noisy
 * {@code job.events} stream into a clean {@code order.events} stream with one event per change.
 */
@Service
public class JobProgress {

    private static final Logger log = LoggerFactory.getLogger(JobProgress.class);

    private record JobRow(UUID orderId, String sku, String status, String robotId, int attempt) {
    }

    private record Change(String status, OrderEventType type, String robotId, String reason) {
    }

    private final JdbcClient jdbc;
    private final Outbox outbox;

    public JobProgress(JdbcClient jdbc, Outbox outbox) {
        this.jdbc = jdbc;
        this.outbox = outbox;
    }

    @Transactional
    public void apply(JobEvent event) {
        UUID jobId = UUID.fromString(event.jobId());
        // Lock the order first, then the job: every writer takes them in this order, so no deadlocks.
        Optional<String> orderStatus = jdbc.sql("""
                        select o.status from orders o join pick_jobs j on j.order_id = o.id
                        where j.id = :id for update of o
                        """)
                .param("id", jobId).query(String.class).optional();
        if (orderStatus.isEmpty()) {
            log.warn("Ignoring {} for unknown job {}", event.type(), event.jobId());
            return;
        }
        JobRow job = jdbc.sql("select order_id, sku, status, robot_id, attempt from pick_jobs where id = :id for update")
                .param("id", jobId)
                .query((rs, n) -> new JobRow(rs.getObject("order_id", UUID.class), rs.getString("sku"),
                        rs.getString("status"), rs.getString("robot_id"), rs.getInt("attempt")))
                .single();

        if (isFinal(job.status()) || event.attempt() < job.attempt()) {
            return; // finished, or a late event from an earlier attempt
        }
        Change change = switch (event.type()) {
            case JOB_ASSIGNED -> new Change("ASSIGNED", OrderEventType.JOB_ASSIGNED, event.robotId(), null);
            case JOB_PICKED -> new Change("PICKED", OrderEventType.JOB_PICKED, event.robotId(), null);
            case JOB_COMPLETED -> new Change("COMPLETED", OrderEventType.JOB_COMPLETED, event.robotId(), null);
            case JOB_REQUEUED -> new Change("PENDING", OrderEventType.JOB_REQUEUED, null, event.reason());
            case JOB_DEAD_LETTERED -> new Change("FAILED", OrderEventType.JOB_FAILED, event.robotId(), event.reason());
        };
        if (event.attempt() == job.attempt()) {
            if (change.status().equals(job.status()) && Objects.equals(change.robotId(), job.robotId())) {
                return; // duplicate
            }
            if (rank(change.status()) < rank(job.status()) && !change.status().equals("PENDING")) {
                return; // for example a repeated JOB_ASSIGNED after JOB_PICKED
            }
        }

        Instant now = Instant.now();
        jdbc.sql("""
                        update pick_jobs set status = :status, robot_id = :robot, attempt = :attempt,
                               reason = coalesce(:reason, reason), updated_at = :now
                        where id = :id
                        """)
                .param("status", change.status())
                .param("robot", change.robotId())
                .param("attempt", Math.max(event.attempt(), job.attempt()))
                .param("reason", change.reason())
                .param("now", Timestamp.from(now))
                .param("id", jobId)
                .update();
        emit(change.type(), job.orderId(), jobId, job.sku(), change.robotId(), change.reason(), now);
        updateOrder(job.orderId(), orderStatus.get(), change, now);
    }

    private void updateOrder(UUID orderId, String current, Change change, Instant now) {
        if (isFinal(current)) {
            return;
        }
        String next;
        OrderEventType type;
        if (change.status().equals("FAILED")) {
            next = "FAILED";
            type = OrderEventType.ORDER_FAILED;
        } else if (change.status().equals("COMPLETED") && jdbc.sql(
                        "select count(*) from pick_jobs where order_id = :id and status <> 'COMPLETED'")
                .param("id", orderId).query(Long.class).single() == 0) {
            next = "COMPLETED";
            type = OrderEventType.ORDER_COMPLETED;
        } else if (current.equals("NEW") && !change.status().equals("PENDING")) {
            next = "IN_PROGRESS";
            type = OrderEventType.ORDER_STARTED;
        } else {
            return;
        }
        jdbc.sql("update orders set status = :status, updated_at = :now where id = :id")
                .param("status", next).param("now", Timestamp.from(now)).param("id", orderId)
                .update();
        // An order that finishes straight from NEW (events skipped or merged) still gets ORDER_STARTED first.
        if (current.equals("NEW") && type != OrderEventType.ORDER_STARTED) {
            emit(OrderEventType.ORDER_STARTED, orderId, null, null, null, null, now);
        }
        emit(type, orderId, null, null, null, type == OrderEventType.ORDER_FAILED ? change.reason() : null, now);
    }

    private void emit(OrderEventType type, UUID orderId, UUID jobId, String sku, String robotId, String reason, Instant at) {
        UUID eventId = UUID.randomUUID();
        outbox.add(Destination.KAFKA, Topics.KAFKA_ORDER_EVENTS, orderId.toString(), eventId,
                new OrderEvent(eventId.toString(), type, orderId.toString(), jobId == null ? null : jobId.toString(),
                        sku, robotId, reason, at.toEpochMilli()));
    }

    private static boolean isFinal(String status) {
        return status.equals("COMPLETED") || status.equals("FAILED");
    }

    private static int rank(String status) {
        return switch (status) {
            case "PENDING" -> 0;
            case "ASSIGNED" -> 1;
            case "PICKED" -> 2;
            default -> 3;
        };
    }
}
