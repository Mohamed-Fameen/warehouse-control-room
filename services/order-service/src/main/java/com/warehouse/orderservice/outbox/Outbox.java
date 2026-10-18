package com.warehouse.orderservice.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Writes messages to the outbox table. Must run inside the transaction that makes the change. */
@Component
public class Outbox {

    public enum Destination { KAFKA, RABBIT }

    private final JdbcClient jdbc;
    private final ObjectMapper json;

    public Outbox(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /**
     * @param topic Kafka topic, or RabbitMQ exchange
     * @param key   Kafka record key, or RabbitMQ routing key
     * @param messageId unique ID consumers use to drop duplicates
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void add(Destination destination, String topic, String key, UUID messageId, Object payload) {
        jdbc.sql("""
                insert into outbox (destination, topic, message_key, message_id, payload)
                values (:destination, :topic, :key, :messageId, :payload)
                """)
                .param("destination", destination.name())
                .param("topic", topic)
                .param("key", key)
                .param("messageId", messageId)
                .param("payload", toJson(payload))
                .update();
    }

    private String toJson(Object payload) {
        try {
            return json.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
