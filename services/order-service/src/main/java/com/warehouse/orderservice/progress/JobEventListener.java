package com.warehouse.orderservice.progress;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.JobEvent;
import com.warehouse.common.Topics;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reads dispatcher job events. One consumer group across order-service replicas, so each event
 * is applied once (and again only after a crash, which {@link JobProgress} tolerates). Keyed by
 * order ID, so one order's events are applied in order.
 */
@Component
public class JobEventListener {

    private static final Logger log = LoggerFactory.getLogger(JobEventListener.class);

    private final JobProgress progress;
    private final ObjectMapper json;

    public JobEventListener(JobProgress progress, ObjectMapper json) {
        this.progress = progress;
        this.json = json;
    }

    @KafkaListener(topics = Topics.KAFKA_JOB_EVENTS, groupId = "order-service")
    void onEvent(String payload) {
        JobEvent event;
        try {
            event = json.readValue(payload, JobEvent.class);
        } catch (IOException e) {
            log.warn("Skipping unreadable job event: {}", e.getMessage());
            return;
        }
        progress.apply(event); // a database error is retried by the container's error handler
    }
}
