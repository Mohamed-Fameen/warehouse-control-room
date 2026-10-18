package com.warehouse.common;

public enum JobEventType {
    /** A robot accepted the job. */
    JOB_ASSIGNED,
    /** The robot picked the item at the shelf. */
    JOB_PICKED,
    /** The robot dropped the item at a pack station. Final. */
    JOB_COMPLETED,
    /** The attempt failed (robot failed, broke, went offline, timed out); the job is back on the queue. */
    JOB_REQUEUED,
    /** Out of attempts: the job was rejected to the dead-letter queue. Final. */
    JOB_DEAD_LETTERED
}
