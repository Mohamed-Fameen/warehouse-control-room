package com.warehouse.orderservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.JobEvent;
import com.warehouse.common.JobEventType;
import com.warehouse.common.OrderEvent;
import com.warehouse.common.OrderEventType;
import com.warehouse.common.PickJobMessage;
import com.warehouse.common.Queues;
import com.warehouse.common.Topics;
import com.warehouse.orderservice.order.OrderView;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** HTTP in; Postgres rows, RabbitMQ jobs and Kafka events out. Skipped when Docker is not running. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "orders.generator.per-minute=0")
@Testcontainers(disabledWithoutDocker = true)
class OrderFlowIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.9");

    @Container
    @ServiceConnection
    static final RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:4.3.6");

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");

    @BeforeAll
    static void createTopic() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.KAFKA_ORDER_EVENTS, 6, (short) 1),
                    new NewTopic(Topics.KAFKA_JOB_EVENTS, 6, (short) 1))).all().get();
        }
    }

    @Autowired
    TestRestTemplate http;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper json;

    @Test
    void orderBecomesJobsOnRabbitAndEventsOnKafka() throws Exception {
        ResponseEntity<OrderView> created = http.postForEntity("/api/orders", Map.of("lines", List.of(
                Map.of("sku", "SKU-0001", "qty", 2),
                Map.of("sku", "SKU-0042", "qty", 1))), OrderView.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        OrderView order = created.getBody();
        assertThat(order.jobs()).hasSize(2).allMatch(j -> j.status().equals("PENDING"));
        assertThat(http.getForObject("/api/orders/" + order.id(), OrderView.class).jobs()).hasSize(2);

        // RabbitMQ: one message per job; message ID is the job ID. Skip jobs of other tests' orders.
        List<PickJobMessage> jobs = new ArrayList<>();
        while (jobs.size() < 2) {
            Message message = rabbitTemplate.receive(Queues.PICK_JOBS, 15_000);
            assertThat(message).as("pick job %d", jobs.size()).isNotNull();
            PickJobMessage job = json.readValue(message.getBody(), PickJobMessage.class);
            assertThat(message.getMessageProperties().getMessageId()).isEqualTo(job.jobId());
            if (job.orderId().equals(order.id().toString())) {
                jobs.add(job);
            }
        }
        assertThat(jobs).extracting(PickJobMessage::sku).containsExactlyInAnyOrder("SKU-0001", "SKU-0042");
        assertThat(jobs).allMatch(j -> j.orderId().equals(order.id().toString()));

        // Kafka: JOB_CREATED x2 then ORDER_CREATED, all keyed by order ID (so in one partition, in order).
        List<ConsumerRecord<String, String>> events = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "order-it",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(Set.of(Topics.KAFKA_ORDER_EVENTS));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (events.size() < 3 && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (r.key().equals(order.id().toString())) {
                        events.add(r);
                    }
                });
            }
        }
        assertThat(events).hasSize(3).allMatch(r -> r.key().equals(order.id().toString()));
        List<OrderEventType> types = new ArrayList<>();
        for (ConsumerRecord<String, String> r : events) {
            types.add(json.readValue(r.value(), OrderEvent.class).type());
        }
        assertThat(types).containsExactly(OrderEventType.JOB_CREATED, OrderEventType.JOB_CREATED, OrderEventType.ORDER_CREATED);

        assertThat(jdbc.sql("select count(*) from outbox where published_at is null").query(Long.class).single()).isZero();
    }

    @Test
    void badLineRollsBackTheWholeOrder() {
        long orders = count("orders");
        long outbox = count("outbox");

        ResponseEntity<String> response = http.postForEntity("/api/orders", Map.of("lines", List.of(
                Map.of("sku", "SKU-0001", "qty", 1),
                Map.of("sku", "SKU-9999", "qty", 1))), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("unknown SKU: SKU-9999");
        assertThat(count("orders")).isEqualTo(orders);
        assertThat(count("outbox")).isEqualTo(outbox); // no job was sent for the valid first line
    }

    private long count(String table) {
        return jdbc.sql("select count(*) from " + table).query(Long.class).single();
    }

    @Test
    void jobEventsDriveJobAndOrderStatusAndBecomeOrderEvents() throws Exception {
        OrderView order = createOrder("SKU-0003", "SKU-0004");
        String a = order.jobs().get(0).id().toString();
        String b = order.jobs().get(1).id().toString();

        publish(
                jobEvent(JobEventType.JOB_ASSIGNED, order, a, "r-01", null, 1),
                jobEvent(JobEventType.JOB_ASSIGNED, order, a, "r-01", null, 1),       // duplicate
                jobEvent(JobEventType.JOB_ASSIGNED, order, b, "r-02", null, 1),
                jobEvent(JobEventType.JOB_REQUEUED, order, b, "r-02", "r-02 is OFFLINE", 1),
                jobEvent(JobEventType.JOB_ASSIGNED, order, b, "r-03", null, 2),
                jobEvent(JobEventType.JOB_PICKED, order, b, "r-02", null, 1),          // late, earlier attempt
                jobEvent(JobEventType.JOB_PICKED, order, a, "r-01", null, 1),
                jobEvent(JobEventType.JOB_COMPLETED, order, a, "r-01", null, 1),
                jobEvent(JobEventType.JOB_PICKED, order, b, "r-03", null, 2),
                jobEvent(JobEventType.JOB_COMPLETED, order, b, "r-03", null, 2),
                jobEvent(JobEventType.JOB_COMPLETED, order, b, "r-03", null, 2));     // duplicate

        OrderView done = awaitStatus(order, "COMPLETED");
        OrderView.Job jobB = done.jobs().stream().filter(j -> j.id().toString().equals(b)).findFirst().orElseThrow();
        assertThat(jobB.robotId()).isEqualTo("r-03");
        assertThat(jobB.attempt()).isEqualTo(2);
        assertThat(jobB.reason()).isEqualTo("r-02 is OFFLINE");

        assertThat(orderEvents(order, OrderEventType.ORDER_COMPLETED)).containsExactly(
                OrderEventType.JOB_CREATED, OrderEventType.JOB_CREATED, OrderEventType.ORDER_CREATED,
                OrderEventType.JOB_ASSIGNED, OrderEventType.ORDER_STARTED,
                OrderEventType.JOB_ASSIGNED,
                OrderEventType.JOB_REQUEUED,
                OrderEventType.JOB_ASSIGNED,
                OrderEventType.JOB_PICKED, OrderEventType.JOB_COMPLETED,
                OrderEventType.JOB_PICKED, OrderEventType.JOB_COMPLETED, OrderEventType.ORDER_COMPLETED);
    }

    @Test
    void aDeadLetteredJobFailsItsOrder() throws Exception {
        OrderView order = createOrder("SKU-0005", "SKU-0006");
        String a = order.jobs().get(0).id().toString();
        String b = order.jobs().get(1).id().toString();

        publish(
                jobEvent(JobEventType.JOB_ASSIGNED, order, a, "r-01", null, 3),
                jobEvent(JobEventType.JOB_DEAD_LETTERED, order, a, "r-01", "r-01 failed it: battery empty (attempt 3, limit 3)", 3),
                jobEvent(JobEventType.JOB_ASSIGNED, order, b, "r-02", null, 1),
                jobEvent(JobEventType.JOB_COMPLETED, order, b, "r-02", null, 1));

        OrderView failed = awaitStatus(order, "FAILED");
        assertThat(failed.jobs()).extracting(OrderView.Job::status).containsExactlyInAnyOrder("FAILED", "COMPLETED");
        List<OrderEvent> events = new ArrayList<>();
        orderEvents(order, OrderEventType.JOB_COMPLETED, events);
        OrderEvent orderFailed = events.stream().filter(e -> e.type() == OrderEventType.ORDER_FAILED).findFirst().orElseThrow();
        assertThat(orderFailed.reason()).contains("battery empty");
    }

    private OrderView createOrder(String... skus) {
        List<Map<String, Object>> lines = new ArrayList<>();
        for (String sku : skus) {
            lines.add(Map.of("sku", sku, "qty", 1));
        }
        return http.postForEntity("/api/orders", Map.of("lines", lines), OrderView.class).getBody();
    }

    private JobEvent jobEvent(JobEventType type, OrderView order, String jobId, String robotId, String reason, int attempt) {
        return new JobEvent(UUID.randomUUID().toString(), type, jobId, order.id().toString(), robotId, reason,
                attempt, System.currentTimeMillis());
    }

    private void publish(JobEvent... events) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", kafka.getBootstrapServers()), new StringSerializer(), new StringSerializer())) {
            for (JobEvent event : events) {
                producer.send(new ProducerRecord<>(Topics.KAFKA_JOB_EVENTS, event.orderId(), json.writeValueAsString(event))).get();
            }
        }
    }

    private OrderView awaitStatus(OrderView order, String status) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        OrderView current = null;
        while (System.nanoTime() < deadline) {
            current = http.getForObject("/api/orders/" + order.id(), OrderView.class);
            if (current.status().equals(status)) {
                return current;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("order " + order.id() + " is " + (current == null ? null : current.status()) + ", expected " + status);
    }

    /** This order's event types from order.events, up to and including the first {@code until}. */
    private List<OrderEventType> orderEvents(OrderView order, OrderEventType until) throws Exception {
        List<OrderEvent> events = new ArrayList<>();
        orderEvents(order, until, events);
        return events.stream().map(OrderEvent::type).toList();
    }

    private void orderEvents(OrderView order, OrderEventType until, List<OrderEvent> into) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "order-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(Set.of(Topics.KAFKA_ORDER_EVENTS));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (r.key().equals(order.id().toString())) {
                        OrderEvent event = json.readValue(r.value(), OrderEvent.class);
                        into.add(event);
                        if (event.type() == until) {
                            return;
                        }
                    }
                }
            }
        }
        throw new AssertionError("no " + until + " for order " + order.id() + "; saw " + into.stream().map(OrderEvent::type).toList());
    }
}
