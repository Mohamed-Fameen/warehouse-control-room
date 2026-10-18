package com.warehouse.dispatcher.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.warehouse.common.PickJobMessage;
import com.warehouse.common.Queues;
import com.warehouse.dispatcher.DispatchLoop;
import com.warehouse.dispatcher.board.JobBoard;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Takes pick jobs from RabbitMQ with manual acknowledgement and hands them to the dispatch loop.
 * The message stays unacknowledged until the job is done (see {@link JobBoard}); the listener
 * prefetch caps how many jobs this dispatcher holds at once, so other replicas get the rest.
 */
@Component
public class PickJobListener {

    private static final Logger log = LoggerFactory.getLogger(PickJobListener.class);

    private final DispatchLoop loop;
    private final ObjectMapper json;

    public PickJobListener(DispatchLoop loop, ObjectMapper json) {
        this.loop = loop;
        this.json = json;
    }

    @RabbitListener(queues = Queues.PICK_JOBS, ackMode = "MANUAL")
    void onMessage(Message message, Channel channel) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        PickJobMessage job;
        try {
            job = json.readValue(message.getBody(), PickJobMessage.class);
        } catch (IOException e) {
            // Retrying cannot fix a broken message: reject without requeue, so it is dead-lettered.
            log.warn("Dead-lettering unreadable pick job: {}", e.getMessage());
            channel.basicReject(tag, false);
            return;
        }
        // Quorum queues count earlier deliveries in x-delivery-count (absent on the first).
        Object count = message.getMessageProperties().getHeader("x-delivery-count");
        int attempt = count instanceof Number n ? n.intValue() + 1 : 1;
        RabbitDelivery delivery = new RabbitDelivery(job, attempt, channel, tag);
        loop.submit(board -> board.onJob(delivery, System.currentTimeMillis()));
    }

    /**
     * Settles one delivery on the channel it came from. If that channel has closed in the
     * meantime, the broker has already requeued the message and the ack is simply dropped.
     */
    private record RabbitDelivery(PickJobMessage job, int attempt, Channel channel, long tag) implements JobBoard.Delivery {

        @Override
        public void ack() {
            settle("ack", () -> channel.basicAck(tag, false));
        }

        @Override
        public void requeue() {
            settle("requeue", () -> channel.basicNack(tag, false, true));
        }

        @Override
        public void deadLetter() {
            settle("dead-letter", () -> channel.basicReject(tag, false));
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        private interface ChannelCall {
            void run() throws IOException;
        }

        private void settle(String what, ChannelCall call) {
            try {
                call.run();
            } catch (IOException | RuntimeException e) {
                log.info("Could not {} {} (channel closed, the broker redelivers it): {}", what, job.jobId(), e.toString());
            }
        }
    }
}
