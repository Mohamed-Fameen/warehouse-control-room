package com.warehouse.gateway.orders;

import static org.assertj.core.api.Assertions.assertThat;

import com.warehouse.common.OrderEvent;
import com.warehouse.common.OrderEventType;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderBoardTest {

    private final OrderBoard board = new OrderBoard(3);

    private static OrderEvent event(OrderEventType type, String orderId, String jobId, String robotId, String reason, long ts) {
        return new OrderEvent(UUID.randomUUID().toString(), type, orderId, jobId,
                jobId == null ? null : "SKU-" + jobId, robotId, reason, ts);
    }

    private void create(String orderId, long ts, String... jobIds) {
        for (String jobId : jobIds) {
            board.apply(event(OrderEventType.JOB_CREATED, orderId, jobId, null, null, ts));
        }
        board.apply(event(OrderEventType.ORDER_CREATED, orderId, null, null, null, ts));
    }

    @Test
    void followsAnOrderThroughItsJobs() {
        create("o-1", 1_000, "j-1", "j-2");
        board.apply(event(OrderEventType.JOB_ASSIGNED, "o-1", "j-1", "r-01", null, 1_100));
        board.apply(event(OrderEventType.ORDER_STARTED, "o-1", null, null, null, 1_100));
        board.apply(event(OrderEventType.JOB_REQUEUED, "o-1", "j-1", null, "r-01 is OFFLINE", 1_200));
        board.apply(event(OrderEventType.JOB_ASSIGNED, "o-1", "j-1", "r-02", null, 1_300));
        board.apply(event(OrderEventType.JOB_PICKED, "o-1", "j-2", "r-03", null, 1_400));

        OrderBoard.OrderCard order = board.snapshot().getFirst();
        assertThat(order.status()).isEqualTo("IN_PROGRESS");
        assertThat(order.updatedAt()).isEqualTo(1_400);
        assertThat(order.jobs()).extracting(OrderBoard.JobCard::id).containsExactly("j-1", "j-2"); // order kept
        OrderBoard.JobCard j1 = order.jobs().getFirst();
        assertThat(j1.status()).isEqualTo("ASSIGNED");
        assertThat(j1.robotId()).isEqualTo("r-02");
        assertThat(j1.reason()).isEqualTo("r-01 is OFFLINE");
        assertThat(j1.sku()).isEqualTo("SKU-j-1");
    }

    @Test
    void recordsFailures() {
        create("o-1", 1_000, "j-1");
        board.apply(event(OrderEventType.JOB_FAILED, "o-1", "j-1", "r-01", "battery empty", 1_100));
        board.apply(event(OrderEventType.ORDER_FAILED, "o-1", null, null, "battery empty", 1_100));

        OrderBoard.OrderCard order = board.snapshot().getFirst();
        assertThat(order.status()).isEqualTo("FAILED");
        assertThat(order.reason()).isEqualTo("battery empty");
        assertThat(order.jobs().getFirst().status()).isEqualTo("FAILED");
    }

    @Test
    void ignoresDuplicatesAndOrdersCreatedBeforeTheWindow() {
        OrderEvent created = event(OrderEventType.JOB_CREATED, "o-1", "j-1", null, null, 1_000);
        board.apply(created);
        board.apply(event(OrderEventType.JOB_ASSIGNED, "o-1", "j-1", "r-01", null, 1_100));
        board.apply(created); // redelivered by the outbox: must not reset the job
        board.apply(event(OrderEventType.JOB_PICKED, "o-old", "j-9", "r-02", null, 1_100));

        assertThat(board.snapshot()).extracting(OrderBoard.OrderCard::id).containsExactly("o-1");
        assertThat(board.snapshot().getFirst().jobs().getFirst().status()).isEqualTo("ASSIGNED");
    }

    @Test
    void reportsChangesOnceAndEvictsOldestFinishedFirst() {
        create("o-1", 1_000, "j-1");
        create("o-2", 2_000, "j-2");
        create("o-3", 3_000, "j-3");
        board.apply(event(OrderEventType.ORDER_COMPLETED, "o-2", null, null, null, 3_100));
        assertThat(board.drainChanges().orders()).extracting(OrderBoard.OrderCard::id).containsExactly("o-3", "o-2", "o-1");
        assertThat(board.drainChanges().isEmpty()).isTrue();

        create("o-4", 4_000, "j-4");

        OrderBoard.Changes changes = board.drainChanges();
        assertThat(changes.orders()).extracting(OrderBoard.OrderCard::id).containsExactly("o-4");
        assertThat(changes.removed()).containsExactly("o-2"); // finished, so it goes before the older o-1
        assertThat(board.snapshot()).extracting(OrderBoard.OrderCard::id).containsExactly("o-4", "o-3", "o-1");
    }
}
