package com.warehouse.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.common.Topics;
import com.warehouse.gateway.fleet.FleetState;
import com.warehouse.gateway.fleet.RobotView;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Kafka in, fleet state out, against a real broker. Skipped when Docker is not running. */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class GatewayKafkaIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    /** Runs before the Spring context starts, so this status is already in Kafka when the gateway boots. */
    @BeforeAll
    static void seed() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.KAFKA_ROBOT_TELEMETRY, 6, (short) 1),
                    new NewTopic(Topics.KAFKA_ROBOT_STATUS, 6, (short) 1)
                            .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }
        try (var producer = producer()) {
            send(producer, Topics.KAFKA_ROBOT_STATUS, "r-09", new RobotStatusMessage("r-09", RobotStatus.OFFLINE, 0));
            producer.send(new ProducerRecord<>(Topics.KAFKA_ROBOT_STATUS, "junk", "not json".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Autowired
    FleetState state;

    @Test
    void rebuildsStatusFromTheBeginningAndFollowsLiveTelemetry() throws Exception {
        try (var producer = producer()) {
            long deadline = System.currentTimeMillis() + 60_000;
            // Telemetry is read from the end, so keep producing until the consumer has caught up.
            while (find("r-01").isEmpty() && System.currentTimeMillis() < deadline) {
                send(producer, Topics.KAFKA_ROBOT_TELEMETRY, "r-01", new RobotTelemetry(
                        "r-01", "dc1", 0, System.currentTimeMillis(), 4.5, 2.5, 0, 1.5, 88, RobotStatus.MOVING));
                Thread.sleep(250);
            }
        }

        assertThat(find("r-01")).hasValueSatisfying(r -> {
            assertThat(r.x()).isEqualTo(4.5);
            assertThat(r.status()).isEqualTo(RobotStatus.MOVING);
        });
        assertThat(find("r-09")).hasValueSatisfying(r -> assertThat(r.status()).isEqualTo(RobotStatus.OFFLINE));
        assertThat(state.snapshot()).extracting(RobotView::id).containsExactly("r-01", "r-09"); // junk skipped
    }

    private Optional<RobotView> find(String id) {
        return state.snapshot().stream().filter(r -> r.id().equals(id)).findFirst();
    }

    private static KafkaProducer<String, byte[]> producer() {
        return new KafkaProducer<>(Map.of("bootstrap.servers", kafka.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer());
    }

    private static void send(KafkaProducer<String, byte[]> producer, String topic, String key, Object value)
            throws Exception {
        producer.send(new ProducerRecord<>(topic, key, new ObjectMapper().writeValueAsBytes(value))).get();
    }
}
