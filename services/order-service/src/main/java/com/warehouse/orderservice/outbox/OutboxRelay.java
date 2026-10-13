package com.warehouse.orderservice.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Publishes outbox rows in ID order (per destination) and marks them published.
 *
 * <p>At least once: if the process dies after a broker accepted a message but before the row
 * is marked, the message is sent again on the next run. Consumers drop duplicates by message ID.
 * {@code FOR UPDATE SKIP LOCKED} lets several order-service replicas relay in parallel without
 * sending the same row twice.
 */
@Component
@ConditionalOnProperty(prefix = "orders.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;
    private static final long CONFIRM_TIMEOUT_MS = 5_000;

    record Row(long id, String destination, String topic, String key, UUID messageId, String payload) {
    }

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final RabbitTemplate rabbit;
    private final MeterRegistry meters;

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, RabbitTemplate rabbit, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.rabbit = rabbit;
        this.meters = meters;
    }

    @Scheduled(fixedDelayString = "${orders.outbox.poll-interval:PT0.2S}")
    @Transactional
    public void relay() {
        List<Row> rows = jdbc.sql("""
                select id, destination, topic, message_key, message_id, payload
                from outbox
                where published_at is null
                order by id
                limit :batch
                for update skip locked
                """)
                .param("batch", BATCH)
                .query((rs, n) -> new Row(rs.getLong("id"), rs.getString("destination"), rs.getString("topic"),
                        rs.getString("message_key"), rs.getObject("message_id", UUID.class), rs.getString("payload")))
                .list();
        if (rows.isEmpty()) {
            return;
        }

        List<Long> sent = new ArrayList<>();
        Set<String> failed = new HashSet<>();
        for (Row row : rows) {
            if (failed.contains(row.destination())) {
                continue; // keep this destination's order: nothing overtakes the failed row
            }
            try {
                publish(row);
                sent.add(row.id());
            } catch (Exception e) {
                // Retried next run. Other destinations carry on, so a RabbitMQ outage does not hold up Kafka.
                failed.add(row.destination());
                log.warn("Outbox row {} ({} {}) not published: {}", row.id(), row.destination(), row.topic(), e.toString());
                meters.counter("outbox.published", "destination", row.destination(), "outcome", "failed").increment();
            }
        }
        if (!sent.isEmpty()) {
            jdbc.sql("update outbox set published_at = now() where id in (:ids)").param("ids", sent).update();
        }
    }

    private void publish(Row row) throws Exception {
        switch (row.destination()) {
            case "KAFKA" -> {
                ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.key(), row.payload());
                record.headers().add("message_id", row.messageId().toString().getBytes(StandardCharsets.UTF_8));
                kafka.send(record).get(CONFIRM_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            case "RABBIT" -> {
                var message = MessageBuilder.withBody(row.payload().getBytes(StandardCharsets.UTF_8))
                        .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                        .setMessageId(row.messageId().toString())
                        .build(); // persistent delivery mode by default
                // Publisher confirm: the broker has taken responsibility for the message (for a
                // quorum queue, a majority of replicas have it) before we mark the row published.
                rabbit.invoke(ops -> {
                    ops.send(row.topic(), row.key(), message);
                    ops.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
                    return null;
                });
            }
            default -> throw new IllegalStateException("unknown destination " + row.destination());
        }
        meters.counter("outbox.published", "destination", row.destination(), "outcome", "ok").increment();
    }

    /** Published rows are only useful for debugging; keep a day of them. */
    @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT1M")
    public void purge() {
        int deleted = jdbc.sql("delete from outbox where published_at < now() - interval '1 day'").update();
        if (deleted > 0) {
            log.info("Purged {} published outbox rows", deleted);
        }
    }
}
