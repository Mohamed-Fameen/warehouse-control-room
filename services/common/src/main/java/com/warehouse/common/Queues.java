package com.warehouse.common;

/**
 * RabbitMQ names shared by the order service (publisher) and dispatchers (consumers).
 *
 * <pre>
 *   wh.jobs (direct) --pick--> pick-jobs (quorum, delivery limit) --dead letter--> wh.jobs.dlx --pick--> pick-jobs.dlq
 * </pre>
 */
public final class Queues {

    private Queues() {
    }

    /** Direct exchange the order service publishes pick jobs to. */
    public static final String JOBS_EXCHANGE = "wh.jobs";

    /** Routing key for pick jobs, on both the main and the dead-letter exchange. */
    public static final String PICK_ROUTING_KEY = "pick";

    /** Work queue of {@link PickJobMessage}; each message goes to exactly one dispatcher. */
    public static final String PICK_JOBS = "pick-jobs";

    /** Dead-letter exchange: messages rejected without requeue, or redelivered too often, go here. */
    public static final String JOBS_DLX = "wh.jobs.dlx";

    /** Dead-letter queue for pick jobs; the UI's DLQ panel reads it in Phase 3. */
    public static final String PICK_JOBS_DLQ = "pick-jobs.dlq";

    /** After this many deliveries a pick job is dead-lettered instead of redelivered. */
    public static final int PICK_JOBS_DELIVERY_LIMIT = 5;
}
