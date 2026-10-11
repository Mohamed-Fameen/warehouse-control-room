package com.warehouse.bridge;

import com.warehouse.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Maps an MQTT message to a Kafka record and sends it. The payload bytes are forwarded as they
 * are; the bridge does not parse JSON.
 */
@Component
public class KafkaForwarder {

    private static final Logger log = LoggerFactory.getLogger(KafkaForwarder.class);

    /** Kafka header carrying the original MQTT topic. */
    public static final String HEADER_MQTT_TOPIC = "mqtt_topic";

    /** Where an MQTT topic goes in Kafka. */
    record Route(String kafkaTopic, String key) {
    }

    private final KafkaTemplate<String, byte[]> kafka;
    private final MeterRegistry meters;
    private final Counter ignored;

    public KafkaForwarder(KafkaTemplate<String, byte[]> kafka, MeterRegistry meters) {
        this.kafka = kafka;
        this.meters = meters;
        this.ignored = meters.counter("bridge.messages.ignored");
    }

    /**
     * {@code wh/{site}/robot/{id}/telemetry} goes to {@link Topics#KAFKA_ROBOT_TELEMETRY} and
     * {@code wh/{site}/robot/{id}/status} to {@link Topics#KAFKA_ROBOT_STATUS}, both keyed by robot ID.
     */
    static Optional<Route> route(String mqttTopic) {
        String[] parts = mqttTopic.split("/", -1);
        if (parts.length != 5 || !parts[0].equals("wh") || !parts[2].equals("robot") || parts[3].isEmpty()) {
            return Optional.empty();
        }
        return switch (parts[4]) {
            case "telemetry" -> Optional.of(new Route(Topics.KAFKA_ROBOT_TELEMETRY, parts[3]));
            case "status" -> Optional.of(new Route(Topics.KAFKA_ROBOT_STATUS, parts[3]));
            default -> Optional.empty();
        };
    }

    /**
     * Sends asynchronously. Called on the MQTT client's thread, so it must not block for long;
     * {@code max.block.ms} bounds the wait when Kafka is unreachable.
     */
    public void forward(String mqttTopic, byte[] payload) {
        Optional<Route> route = route(mqttTopic);
        if (route.isEmpty()) {
            ignored.increment();
            log.debug("No route for MQTT topic {}", mqttTopic);
            return;
        }
        String kafkaTopic = route.get().kafkaTopic();
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(kafkaTopic, route.get().key(), payload);
        record.headers().add(HEADER_MQTT_TOPIC, mqttTopic.getBytes(StandardCharsets.UTF_8));
        try {
            kafka.send(record).whenComplete((result, error) -> count(kafkaTopic, error));
        } catch (RuntimeException e) {
            count(kafkaTopic, e);
        }
    }

    private void count(String kafkaTopic, Throwable error) {
        String outcome = error == null ? "ok" : "failed";
        meters.counter("bridge.messages.forwarded", "topic", kafkaTopic, "outcome", outcome).increment();
        if (error != null) {
            log.warn("Kafka send to {} failed: {}", kafkaTopic, error.toString());
        }
    }
}
