package com.warehouse.dispatcher.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.JobEvent;
import com.warehouse.common.Topics;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Sends {@link JobEvent}s to Kafka {@code job.events}, keyed by order ID. */
@Component
public class JobEventProducer {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;

    public JobEventProducer(KafkaTemplate<String, String> kafka, ObjectMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    /** Completes when the broker has the event (acks=all), or exceptionally. Never throws. */
    public CompletableFuture<?> send(JobEvent event) {
        try {
            return kafka.send(Topics.KAFKA_JOB_EVENTS, event.orderId(), json.writeValueAsString(event));
        } catch (JsonProcessingException | RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
