package com.warehouse.gateway.fleet;

import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;

/**
 * What the browser gets for one robot. Position fields are null for a robot known only from
 * {@code robot.status} (for example one that was already offline when the gateway started).
 *
 * @param lastSeen epoch ms of the latest telemetry, or null if none yet
 * @param statusTs epoch ms of the status, used to drop telemetry older than an OFFLINE
 */
public record RobotView(
        String id,
        Double x,
        Double y,
        Double heading,
        Double speed,
        Double battery,
        RobotStatus status,
        Long lastSeen,
        long statusTs) {

    static RobotView of(RobotTelemetry t) {
        return new RobotView(t.robotId(), t.x(), t.y(), t.heading(), t.speed(), t.battery(),
                t.status(), t.ts(), t.ts());
    }

    static RobotView of(RobotStatusMessage s) {
        return new RobotView(s.robotId(), null, null, null, null, null, s.status(), null, s.ts());
    }

    RobotView withStatus(RobotStatusMessage s) {
        return new RobotView(id, x, y, heading, speed, battery, s.status(), lastSeen, s.ts());
    }
}
