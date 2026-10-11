package com.warehouse.simulator.world;

/**
 * Physical and behavioural tuning shared by every robot.
 *
 * @param speed              metres per second when moving
 * @param drainMovingPerSec  battery percent lost per second while moving
 * @param drainIdlePerSec    battery percent lost per second otherwise
 * @param chargePerSec       battery percent gained per second on a charger
 * @param lowBattery         at or below this, the robot goes to charge after its current job
 * @param chargeTarget       charging stops at this level
 * @param pickSeconds        time spent at a shelf
 * @param dropSeconds        time spent at a pack station
 */
public record RobotSpec(
        double speed,
        double drainMovingPerSec,
        double drainIdlePerSec,
        double chargePerSec,
        double lowBattery,
        double chargeTarget,
        double pickSeconds,
        double dropSeconds) {
}
