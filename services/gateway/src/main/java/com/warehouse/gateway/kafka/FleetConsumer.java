package com.warehouse.gateway.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.common.Topics;
import com.warehouse.gateway.fleet.FleetState;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Reads robot state from Kafka into {@link FleetState}.
 *
 * <p>No consumer group: every gateway replica serves its own browsers, so each one needs every
 * robot (a broadcast, not a work split). The consumer assigns itself all partitions, reads the
 * compacted {@code robot.status} from the beginning to rebuild the latest status per robot, and
 * reads {@code robot.telemetry} from the end because old positions are useless. Nothing is
 * committed.
 */
@Component
@ConditionalOnProperty(prefix = "gateway", name = "kafka-enabled", havingValue = "true", matchIfMissing = true)
public class FleetConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(FleetConsumer.class);

    private final KafkaProperties kafkaProperties;
    private final FleetState state;
    private final ObjectMapper json;
    private final MeterRegistry meters;
    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile boolean running;
    private Thread thread;

    public FleetConsumer(KafkaProperties kafkaProperties, FleetState state, ObjectMapper json, MeterRegistry meters) {
        this.kafkaProperties = kafkaProperties;
        this.state = state;
        this.json = json;
        this.meters = meters;
    }

    @Override
    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("fleet-consumer").start(this::run);
    }

    private void run() {
        Map<String, Object> config = kafkaProperties.buildConsumerProperties();
        config.remove(ConsumerConfig.GROUP_ID_CONFIG);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer = c;
            List<TopicPartition> status = partitions(c, Topics.KAFKA_ROBOT_STATUS);
            List<TopicPartition> telemetry = partitions(c, Topics.KAFKA_ROBOT_TELEMETRY);
            List<TopicPartition> all = new ArrayList<>(status);
            all.addAll(telemetry);
            c.assign(all);
            c.seekToBeginning(status);
            c.seekToEnd(telemetry);
            log.info("Consuming {} status and {} telemetry partitions", status.size(), telemetry.size());
            while (running) {
                for (ConsumerRecord<String, byte[]> record : c.poll(Duration.ofMillis(500))) {
                    handle(record);
                }
            }
        } catch (WakeupException e) {
            // stop() called wakeup()
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.error("Fleet consumer died", e);
        }
    }

    /** Waits until the topic exists; {@code kafka-init} may still be creating it. */
    private List<TopicPartition> partitions(KafkaConsumer<?, ?> c, String topic) throws InterruptedException {
        while (true) {
            try {
                var infos = c.partitionsFor(topic, Duration.ofSeconds(5));
                if (infos != null && !infos.isEmpty()) {
                    return infos.stream().map(p -> new TopicPartition(topic, p.partition())).toList();
                }
                log.info("Waiting for Kafka topic {}", topic);
            } catch (WakeupException e) {
                throw e;
            } catch (RuntimeException e) {
                log.warn("Cannot list partitions of {} ({}), retrying", topic, e.toString());
            }
            Thread.sleep(2_000);
        }
    }

    private void handle(ConsumerRecord<String, byte[]> record) {
        try {
            if (record.topic().equals(Topics.KAFKA_ROBOT_TELEMETRY)) {
                state.apply(json.readValue(record.value(), RobotTelemetry.class));
            } else {
                state.apply(json.readValue(record.value(), RobotStatusMessage.class));
            }
            meters.counter("gateway.records", "topic", record.topic(), "outcome", "ok").increment();
        } catch (Exception e) {
            // Skip and move on: one bad record must not stop the live view.
            meters.counter("gateway.records", "topic", record.topic(), "outcome", "unparseable").increment();
            log.debug("Skipping unparseable record {}-{}@{}: {}",
                    record.topic(), record.partition(), record.offset(), e.toString());
        }
    }

    @Override
    public void stop() {
        running = false;
        KafkaConsumer<String, byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
        if (thread != null) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
