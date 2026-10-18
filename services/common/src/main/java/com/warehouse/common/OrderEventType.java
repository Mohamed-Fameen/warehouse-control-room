package com.warehouse.common;

public enum OrderEventType {
    ORDER_CREATED,
    /** The first of the order's jobs was assigned to a robot. */
    ORDER_STARTED,
    /** Every job completed. */
    ORDER_COMPLETED,
    /** A job was dead-lettered, so the order cannot complete. */
    ORDER_FAILED,
    JOB_CREATED,
    JOB_ASSIGNED,
    JOB_PICKED,
    JOB_REQUEUED,
    JOB_COMPLETED,
    /** The job was dead-lettered. */
    JOB_FAILED
}
