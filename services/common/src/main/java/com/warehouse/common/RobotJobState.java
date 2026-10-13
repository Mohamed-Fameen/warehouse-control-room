package com.warehouse.common;

public enum RobotJobState {
    /** The robot took the job and is driving to the shelf. */
    ACCEPTED,
    /** The robot did not take the job ({@code reason} says why). Never retained. */
    REJECTED,
    /** Picked at the shelf, driving to a pack station. */
    PICKED,
    /** Dropped at a pack station. The job is done. */
    COMPLETED,
    /** The robot gave up on the job, for example because its battery ran out. */
    FAILED;

    /** True while the robot is still working on the job. */
    public boolean active() {
        return this == ACCEPTED || this == PICKED;
    }
}
