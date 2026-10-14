package com.warehouse.dispatcher.board;

import static org.assertj.core.api.Assertions.assertThat;

import com.warehouse.common.JobEvent;
import com.warehouse.common.JobEventType;
import com.warehouse.common.PickJobMessage;
import com.warehouse.common.RobotCommand;
import com.warehouse.common.RobotJobState;
import com.warehouse.common.RobotJobUpdate;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.common.layout.Cell;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobBoardTest {

    private static final JobBoard.Settings SETTINGS = new JobBoard.Settings(
            Duration.ofSeconds(5), Duration.ofMinutes(3), 45, Duration.ofSeconds(5),
            Duration.ofSeconds(5), Duration.ofMinutes(10), 3);

    private record Sent(String robotId, RobotCommand command) {
    }

    private static final class FakeDelivery implements JobBoard.Delivery {
        final PickJobMessage job;
        final int attempt;
        boolean open = true;
        int acks;
        int requeues;
        int deadLetters;

        FakeDelivery(String jobId, Cell pick) {
            this(jobId, pick, 1);
        }

        FakeDelivery(String jobId, Cell pick, int attempt) {
            this.job = new PickJobMessage(jobId, "order-1", "SKU-0001", 1, pick, 0);
            this.attempt = attempt;
        }

        @Override
        public int attempt() {
            return attempt;
        }

        @Override
        public void deadLetter() {
            deadLetters++;
        }

        @Override
        public PickJobMessage job() {
            return job;
        }

        @Override
        public void ack() {
            acks++;
        }

        @Override
        public void requeue() {
            requeues++;
        }

        @Override
        public boolean isOpen() {
            return open;
        }
    }

    private final List<Sent> sent = new ArrayList<>();
    private final List<JobEvent> published = new ArrayList<>();
    /** Whether Kafka confirms publishes; tests flip it to simulate an outage. */
    private boolean kafkaUp = true;
    private final JobBoard.EventPublisher publisher = (event, confirmed) -> {
        if (kafkaUp) {
            published.add(event);
        }
        confirmed.accept(kafkaUp);
    };
    private final JobBoard board = new JobBoard(SETTINGS, (robotId, command) -> sent.add(new Sent(robotId, command)), publisher);
    private long now = 1_000_000;

    {
        board.holdAssignmentsUntil(0);
    }

    private void robot(String id, double x, double y, RobotStatus status, double battery) {
        board.onTelemetry(new RobotTelemetry(id, "dc1", 0, now, x, y, 0, 0, battery, status), now);
    }

    private void update(String robotId, String jobId, RobotJobState state) {
        board.onJobUpdate(robotId, new RobotJobUpdate(robotId, jobId, state, null, now), now);
    }

    private FakeDelivery job(String jobId, Cell pick) {
        FakeDelivery delivery = new FakeDelivery(jobId, pick);
        board.onJob(delivery, now);
        return delivery;
    }

    @Test
    void sendsEachJobToTheNearestFreeRobot() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        robot("r-02", 30.5, 1.5, RobotStatus.CHARGING, 90);
        job("j-1", new Cell(29, 5));
        job("j-2", new Cell(3, 5));

        board.tick(now);

        assertThat(sent).extracting(Sent::robotId).containsExactly("r-02", "r-01");
        assertThat(sent.getFirst().command().jobId()).isEqualTo("j-1");
        assertThat(sent.getFirst().command().pick()).isEqualTo(new Cell(29, 5));
    }

    @Test
    void acksOnlyWhenTheRobotCompletes() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        FakeDelivery d = job("j-1", new Cell(3, 5));
        board.tick(now);

        update("r-01", "j-1", RobotJobState.ACCEPTED);
        update("r-01", "j-1", RobotJobState.PICKED);
        assertThat(d.acks).isZero();

        update("r-01", "j-1", RobotJobState.COMPLETED);
        assertThat(d.acks).isEqualTo(1);
        assertThat(d.requeues).isZero();
        assertThat(board.snapshot().jobs()).isEmpty();
    }

    @Test
    void skipsRobotsThatCannotTakeAJob() {
        robot("r-low", 2.5, 1.5, RobotStatus.IDLE, 44);
        robot("r-moving", 2.5, 1.5, RobotStatus.MOVING, 90);
        robot("r-broken", 2.5, 1.5, RobotStatus.BROKEN, 90);
        robot("r-stale", 2.5, 1.5, RobotStatus.IDLE, 90);
        robot("r-busy", 2.5, 1.5, RobotStatus.IDLE, 90);
        update("r-busy", "other-dispatchers-job", RobotJobState.ACCEPTED);
        now += 6_000;
        robot("r-low", 2.5, 1.5, RobotStatus.IDLE, 44);
        robot("r-moving", 2.5, 1.5, RobotStatus.MOVING, 90);
        robot("r-broken", 2.5, 1.5, RobotStatus.BROKEN, 90);
        robot("r-busy", 2.5, 1.5, RobotStatus.IDLE, 90);
        job("j-1", new Cell(3, 5));

        board.tick(now);

        assertThat(sent).isEmpty();
    }

    @Test
    void aRejectedJobGoesToAnotherRobot() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        robot("r-02", 10.5, 1.5, RobotStatus.IDLE, 90);
        job("j-1", new Cell(3, 5));
        board.tick(now);
        assertThat(sent.getLast().robotId()).isEqualTo("r-01");

        update("r-01", "j-1", RobotJobState.REJECTED);
        board.tick(now);

        assertThat(sent.getLast().robotId()).isEqualTo("r-02");
    }

    @Test
    void anUnansweredAssignmentTimesOut() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        robot("r-02", 10.5, 1.5, RobotStatus.IDLE, 90);
        job("j-1", new Cell(3, 5));
        board.tick(now);

        now += 5_000;
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        robot("r-02", 10.5, 1.5, RobotStatus.IDLE, 90);
        board.tick(now);

        assertThat(sent).extracting(Sent::robotId).containsExactly("r-01", "r-02");
    }

    @Test
    void requeuesWhenTheRobotFailsOrGoesOffline() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        robot("r-02", 10.5, 1.5, RobotStatus.IDLE, 90);
        FakeDelivery d1 = job("j-1", new Cell(3, 5));
        FakeDelivery d2 = job("j-2", new Cell(11, 5));
        board.tick(now);
        update("r-01", "j-1", RobotJobState.ACCEPTED);
        update("r-02", "j-2", RobotJobState.ACCEPTED);

        update("r-01", "j-1", RobotJobState.FAILED);
        board.onStatus(new RobotStatusMessage("r-02", RobotStatus.OFFLINE, 0), now);

        assertThat(d1.requeues).isEqualTo(1);
        assertThat(d2.requeues).isEqualTo(1);
        assertThat(d1.acks + d2.acks).isZero();
        assertThat(board.snapshot().jobs()).isEmpty();
    }

    @Test
    void requeuesWhenTheRobotRestartsWithoutItsJob() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        FakeDelivery d = job("j-1", new Cell(3, 5));
        board.tick(now);
        update("r-01", "j-1", RobotJobState.ACCEPTED);

        board.onJobUpdate("r-01", null, now); // retained job cleared on reconnect

        assertThat(d.requeues).isEqualTo(1);
    }

    @Test
    void adoptsARedeliveredJobARobotIsAlreadyRunning() {
        // Another dispatcher sent j-1 to r-01, then died; RabbitMQ redelivers j-1 to us.
        robot("r-01", 2.5, 1.5, RobotStatus.MOVING, 90);
        robot("r-02", 10.5, 1.5, RobotStatus.IDLE, 90);
        update("r-01", "j-1", RobotJobState.ACCEPTED);
        FakeDelivery d = job("j-1", new Cell(3, 5));

        board.tick(now);
        assertThat(sent).isEmpty();

        update("r-01", "j-1", RobotJobState.COMPLETED);
        assertThat(d.acks).isEqualTo(1);
    }

    @Test
    void assignsNothingUntilTheFleetIsKnown() {
        JobBoard fresh = new JobBoard(SETTINGS, (robotId, command) -> sent.add(new Sent(robotId, command)), publisher);
        fresh.onJob(new FakeDelivery("j-1", new Cell(3, 5)), now);
        fresh.onTelemetry(new RobotTelemetry("r-01", "dc1", 0, now, 2.5, 1.5, 0, 0, 90, RobotStatus.IDLE), now);
        fresh.tick(now);
        assertThat(sent).isEmpty();

        fresh.holdAssignmentsUntil(now + 2_000);
        fresh.tick(now + 1_000);
        assertThat(sent).isEmpty();
        fresh.onTelemetry(new RobotTelemetry("r-01", "dc1", 1, now + 2_000, 2.5, 1.5, 0, 0, 90, RobotStatus.IDLE), now + 2_000);
        fresh.tick(now + 2_000);
        assertThat(sent).hasSize(1);
    }

    @Test
    void doesNotAdoptAJobFromARobotThatIsDown() {
        robot("r-01", 2.5, 1.5, RobotStatus.MOVING, 90);
        update("r-01", "j-1", RobotJobState.ACCEPTED);
        board.onStatus(new RobotStatusMessage("r-01", RobotStatus.OFFLINE, 0), now); // retained state stays ACCEPTED
        robot("r-02", 10.5, 1.5, RobotStatus.IDLE, 90);

        FakeDelivery d = job("j-1", new Cell(3, 5)); // the requeued job comes back
        update("r-01", "j-1", RobotJobState.ACCEPTED); // stale retained message replayed
        board.tick(now);

        assertThat(sent).extracting(Sent::robotId).containsExactly("r-02");
        assertThat(d.requeues).isZero();
    }

    @Test
    void acksARedeliveryOfAJobThatIsAlreadyDone() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        update("r-01", "j-1", RobotJobState.COMPLETED);

        FakeDelivery d = job("j-1", new Cell(3, 5));

        assertThat(d.acks).isEqualTo(1);
        board.tick(now);
        assertThat(sent).isEmpty();
    }

    @Test
    void handlesTheSameJobDeliveredTwice() {
        FakeDelivery first = job("j-1", new Cell(3, 5));
        FakeDelivery duplicate = job("j-1", new Cell(3, 5));
        assertThat(duplicate.acks).isEqualTo(1); // duplicate publish, first still held
        assertThat(first.acks).isZero();

        first.open = false; // our channel closed; the broker redelivers
        FakeDelivery again = job("j-1", new Cell(3, 5));
        assertThat(again.acks).isZero();

        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        board.tick(now);
        update("r-01", "j-1", RobotJobState.ACCEPTED);
        update("r-01", "j-1", RobotJobState.COMPLETED);
        assertThat(again.acks).isEqualTo(1);
    }

    @Test
    void ignoresTelemetrySentBeforeTheRobotWentOffline() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        board.onStatus(new RobotStatusMessage("r-01", RobotStatus.OFFLINE, now + 10), now);
        board.onTelemetry(new RobotTelemetry("r-01", "dc1", 1, now + 5, 2.5, 1.5, 0, 0, 90, RobotStatus.IDLE), now);
        job("j-1", new Cell(3, 5));

        board.tick(now);

        assertThat(sent).isEmpty();
        assertThat(board.snapshot().robots().getFirst().status()).isEqualTo(RobotStatus.OFFLINE);
    }

    private List<JobEventType> publishedTypes() {
        return published.stream().map(JobEvent::type).toList();
    }

    @Test
    void reportsEachStepAsAJobEvent() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        job("j-1", new Cell(3, 5));
        board.tick(now);
        update("r-01", "j-1", RobotJobState.ACCEPTED);
        update("r-01", "j-1", RobotJobState.PICKED);
        update("r-01", "j-1", RobotJobState.COMPLETED);

        assertThat(publishedTypes()).containsExactly(
                JobEventType.JOB_ASSIGNED, JobEventType.JOB_PICKED, JobEventType.JOB_COMPLETED);
        assertThat(published).allSatisfy(e -> {
            assertThat(e.jobId()).isEqualTo("j-1");
            assertThat(e.orderId()).isEqualTo("order-1");
            assertThat(e.robotId()).isEqualTo("r-01");
            assertThat(e.attempt()).isEqualTo(1);
        });
    }

    @Test
    void acksOnlyAfterKafkaHasTheCompletion() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        FakeDelivery d = job("j-1", new Cell(3, 5));
        board.tick(now);
        update("r-01", "j-1", RobotJobState.ACCEPTED);

        kafkaUp = false;
        update("r-01", "j-1", RobotJobState.COMPLETED);
        board.tick(now);
        assertThat(d.acks).isZero();
        assertThat(board.snapshot().jobs()).extracting(JobBoard.JobView::phase).containsExactly(JobBoard.Phase.FINISHING);

        kafkaUp = true;
        board.tick(now);
        assertThat(d.acks).isEqualTo(1);
        assertThat(publishedTypes()).endsWith(JobEventType.JOB_COMPLETED);
    }

    @Test
    void deadLettersAJobThatIsOutOfAttempts() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        FakeDelivery d = new FakeDelivery("j-1", new Cell(3, 5), 3);
        board.onJob(d, now);
        board.tick(now);
        update("r-01", "j-1", RobotJobState.ACCEPTED);

        update("r-01", "j-1", RobotJobState.FAILED);

        assertThat(d.deadLetters).isEqualTo(1);
        assertThat(d.requeues).isZero();
        JobEvent last = published.getLast();
        assertThat(last.type()).isEqualTo(JobEventType.JOB_DEAD_LETTERED);
        assertThat(last.reason()).contains("attempt 3 of 3");
    }

    @Test
    void reportsARequeue() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        job("j-1", new Cell(3, 5));
        board.tick(now);
        update("r-01", "j-1", RobotJobState.ACCEPTED);

        board.onStatus(new RobotStatusMessage("r-01", RobotStatus.OFFLINE, 0), now);

        JobEvent last = published.getLast();
        assertThat(last.type()).isEqualTo(JobEventType.JOB_REQUEUED);
        assertThat(last.reason()).isEqualTo("r-01 is OFFLINE");
    }

    @Test
    void reportsCompletionOfAJobThatFinishedWhileNoDispatcherHeldIt() {
        robot("r-01", 2.5, 1.5, RobotStatus.IDLE, 90);
        update("r-01", "j-1", RobotJobState.COMPLETED);

        FakeDelivery d = job("j-1", new Cell(3, 5));

        assertThat(d.acks).isEqualTo(1);
        assertThat(published.getLast().type()).isEqualTo(JobEventType.JOB_COMPLETED);
        assertThat(published.getLast().robotId()).isEqualTo("r-01");
    }
}
