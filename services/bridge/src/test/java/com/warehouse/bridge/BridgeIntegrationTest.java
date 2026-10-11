package com.warehouse.bridge;

import static org.assertj.core.api.Assertions.assertThat;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.warehouse.common.Topics;
import java.nio.charset.StandardCharsets;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** MQTT in, Kafka out, against real Mosquitto and Kafka. Skipped when Docker is not running. */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class BridgeIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:2.0.22")
            .withCopyToContainer(Transferable.of("listener 1883\nallow_anonymous true\n"),
                    "/mosquitto/config/mosquitto.conf")
            .withExposedPorts(1883);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("bridge.mqtt.host", mosquitto::getHost);
        registry.add("bridge.mqtt.port", () -> mosquitto.getMappedPort(1883));
    }

    @BeforeAll
    static void createTopics() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.KAFKA_ROBOT_TELEMETRY, 6, (short) 1),
                    new NewTopic(Topics.KAFKA_ROBOT_STATUS, 6, (short) 1)
                            .configs(Map.of("cleanup.policy", "compact")))).all().get();
        }
    }

    @Test
    void forwardsTelemetryAndRetainedStatusKeyedByRobotId() {
        String telemetry = "{\"robotId\":\"r-07\",\"seq\":1}";
        String status = "{\"robotId\":\"r-07\",\"status\":\"OFFLINE\",\"ts\":0}";

        Mqtt5BlockingClient robot = MqttClient.builder().useMqttVersion5()
                .identifier("test-robot")
                .serverHost(mosquitto.getHost())
                .serverPort(mosquitto.getMappedPort(1883))
                .buildBlocking();
        robot.connect();
        robot.publishWith().topic(Topics.robotStatus("dc1", "r-07")).qos(MqttQos.AT_LEAST_ONCE).retain(true)
                .payload(status.getBytes(StandardCharsets.UTF_8)).send();

        try (KafkaConsumer<String, byte[]> consumer = consumer()) {
            consumer.subscribe(Set.of(Topics.KAFKA_ROBOT_TELEMETRY, Topics.KAFKA_ROBOT_STATUS));
            List<ConsumerRecord<String, byte[]>> seen = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            // QoS 0 telemetry is lost until the bridge has subscribed, so keep publishing.
            while (!hasBoth(seen) && System.nanoTime() < deadline) {
                robot.publishWith().topic(Topics.robotTelemetry("dc1", "r-07"))
                        .payload(telemetry.getBytes(StandardCharsets.UTF_8)).send();
                consumer.poll(Duration.ofMillis(500)).forEach(seen::add);
            }
            robot.disconnect();

            ConsumerRecord<String, byte[]> t = first(seen, Topics.KAFKA_ROBOT_TELEMETRY);
            assertThat(t.key()).isEqualTo("r-07");
            assertThat(new String(t.value(), StandardCharsets.UTF_8)).isEqualTo(telemetry);
            assertThat(new String(t.headers().lastHeader(KafkaForwarder.HEADER_MQTT_TOPIC).value(),
                    StandardCharsets.UTF_8)).isEqualTo("wh/dc1/robot/r-07/telemetry");

            ConsumerRecord<String, byte[]> s = first(seen, Topics.KAFKA_ROBOT_STATUS);
            assertThat(s.key()).isEqualTo("r-07");
            assertThat(new String(s.value(), StandardCharsets.UTF_8)).isEqualTo(status);
        }
    }

    private static KafkaConsumer<String, byte[]> consumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "bridge-it",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer());
    }

    private static boolean hasBoth(List<ConsumerRecord<String, byte[]>> records) {
        return records.stream().anyMatch(r -> r.topic().equals(Topics.KAFKA_ROBOT_TELEMETRY))
                && records.stream().anyMatch(r -> r.topic().equals(Topics.KAFKA_ROBOT_STATUS));
    }

    private static ConsumerRecord<String, byte[]> first(List<ConsumerRecord<String, byte[]>> records, String topic) {
        return records.stream().filter(r -> r.topic().equals(topic)).findFirst()
                .orElseThrow(() -> new AssertionError("nothing on " + topic + " within 60 s"));
    }
}
