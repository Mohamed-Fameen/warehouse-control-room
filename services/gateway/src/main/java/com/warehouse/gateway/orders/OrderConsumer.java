package com.warehouse.gateway.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.OrderEvent;
import com.warehouse.common.Topics;
import com.warehouse.gateway.GatewayProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
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
 * Reads {@code order.events} into the {@link OrderBoard}. Like the fleet consumer: no consumer
 * group, every replica reads everything. It starts {@code gateway.order-window} back in time
 * (Kafka's {@code offsetsForTimes}), so a restarted gateway shows recent orders straight away
 * without replaying the whole topic.
 */
@Component
@ConditionalOnProperty(prefix = "gateway", name = "kafka-enabled", havingValue = "true", matchIfMissing = true)
public class OrderConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final KafkaProperties kafkaProperties;
    private final GatewayProperties props;
    private final OrderBoard board;
    private final ObjectMapper json;
    private final MeterRegistry meters;
    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile boolean running;
    private Thread thread;

    public OrderConsumer(KafkaProperties kafkaProperties, GatewayProperties props, OrderBoard board,
                         ObjectMapper json, MeterRegistry meters) {
        this.kafkaProperties = kafkaProperties;
        this.props = props;
        this.board = board;
        this.json = json;
        this.meters = meters;
    }

    @Override
    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("order-consumer").start(this::run);
    }

    private void run() {
        Map<String, Object> config = kafkaProperties.buildConsumerProperties();
        config.remove(ConsumerConfig.GROUP_ID_CONFIG);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer = c;
            List<TopicPartition> partitions = partitions(c);
            c.assign(partitions);
            seekToWindowStart(c, partitions);
            log.info("Consuming {} order event partitions from {} ago", partitions.size(), props.orderWindow());
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
            log.error("Order consumer died", e);
        }
    }

    private void seekToWindowStart(KafkaConsumer<?, ?> c, List<TopicPartition> partitions) {
        long since = System.currentTimeMillis() - props.orderWindow().toMillis();
        Map<TopicPartition, Long> query = new HashMap<>();
        partitions.forEach(p -> query.put(p, since));
        Map<TopicPartition, OffsetAndTimestamp> offsets = c.offsetsForTimes(query);
        for (TopicPartition p : partitions) {
            OffsetAndTimestamp offset = offsets.get(p);
            if (offset != null) {
                c.seek(p, offset.offset());
            } else {
                c.seekToEnd(List.of(p)); // nothing newer than the window start
            }
        }
    }

    /** Waits until the topic exists; {@code kafka-init} may still be creating it. */
    private List<TopicPartition> partitions(KafkaConsumer<?, ?> c) throws InterruptedException {
        while (true) {
            try {
                var infos = c.partitionsFor(Topics.KAFKA_ORDER_EVENTS, Duration.ofSeconds(5));
                if (infos != null && !infos.isEmpty()) {
                    return infos.stream().map(p -> new TopicPartition(Topics.KAFKA_ORDER_EVENTS, p.partition())).toList();
                }
                log.info("Waiting for Kafka topic {}", Topics.KAFKA_ORDER_EVENTS);
            } catch (WakeupException e) {
                throw e;
            } catch (RuntimeException e) {
                log.warn("Cannot list partitions of {} ({}), retrying", Topics.KAFKA_ORDER_EVENTS, e.toString());
            }
            Thread.sleep(2_000);
        }
    }

    private void handle(ConsumerRecord<String, byte[]> record) {
        try {
            board.apply(json.readValue(record.value(), OrderEvent.class));
            meters.counter("gateway.records", "topic", record.topic(), "outcome", "ok").increment();
        } catch (Exception e) {
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
