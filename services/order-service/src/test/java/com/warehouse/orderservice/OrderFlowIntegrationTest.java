package com.warehouse.orderservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
            admin.createTopics(List.of(new NewTopic(Topics.KAFKA_ORDER_EVENTS, 6, (short) 1))).all().get();
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

        // RabbitMQ: one message per job; message ID is the job ID.
        List<PickJobMessage> jobs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Message message = rabbitTemplate.receive(Queues.PICK_JOBS, 15_000);
            assertThat(message).as("pick job %d", i).isNotNull();
            PickJobMessage job = json.readValue(message.getBody(), PickJobMessage.class);
            assertThat(message.getMessageProperties().getMessageId()).isEqualTo(job.jobId());
            jobs.add(job);
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
                consumer.poll(Duration.ofMillis(500)).forEach(events::add);
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
}
