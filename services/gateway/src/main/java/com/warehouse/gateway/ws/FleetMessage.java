package com.warehouse.gateway.ws;

import com.warehouse.gateway.fleet.RobotView;
import java.util.List;

/**
 * One WebSocket frame.
 *
 * <ul>
 *   <li>{@code snapshot}: sent once on connect; every known robot. Replace local state with it.</li>
 *   <li>{@code delta}: sent at most every frame interval; only robots that changed, each one
 *       complete. Upsert them by {@code id}.</li>
 * </ul>
 *
 * @param ts epoch ms when the gateway built the frame
 */
public record FleetMessage(String type, long ts, List<RobotView> robots) {

    public static FleetMessage snapshot(List<RobotView> robots) {
        return new FleetMessage("snapshot", System.currentTimeMillis(), robots);
    }

    public static FleetMessage delta(List<RobotView> robots) {
        return new FleetMessage("delta", System.currentTimeMillis(), robots);
    }
}
