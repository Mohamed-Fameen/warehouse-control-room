package com.warehouse.orderservice.messaging;

import com.warehouse.common.Queues;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the job exchange and queues (see {@link Queues}). RabbitAdmin declares them on every
 * connect; declaring an existing queue with the same arguments is a no-op.
 *
 * <p>Quorum queues are replicated (Raft) and, unlike classic queues, count redeliveries: after
 * {@link Queues#PICK_JOBS_DELIVERY_LIMIT} deliveries a job is dead-lettered instead of being
 * retried forever, so one poison message cannot block a dispatcher.
 */
@Configuration
public class RabbitTopology {

    @Bean
    Declarables jobTopology() {
        DirectExchange jobs = new DirectExchange(Queues.JOBS_EXCHANGE);
        DirectExchange dlx = new DirectExchange(Queues.JOBS_DLX);
        Queue pickJobs = QueueBuilder.durable(Queues.PICK_JOBS)
                .quorum()
                .deliveryLimit(Queues.PICK_JOBS_DELIVERY_LIMIT)
                .deadLetterExchange(Queues.JOBS_DLX)
                .deadLetterRoutingKey(Queues.PICK_ROUTING_KEY)
                .build();
        Queue dlq = QueueBuilder.durable(Queues.PICK_JOBS_DLQ).quorum().build();
        return new Declarables(jobs, dlx, pickJobs, dlq,
                BindingBuilder.bind(pickJobs).to(jobs).with(Queues.PICK_ROUTING_KEY),
                BindingBuilder.bind(dlq).to(dlx).with(Queues.PICK_ROUTING_KEY));
    }
}
