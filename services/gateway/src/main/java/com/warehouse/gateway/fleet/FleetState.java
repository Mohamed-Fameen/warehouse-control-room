package com.warehouse.gateway.fleet;

import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Latest known state of every robot, plus the set of robots changed since the last frame.
 * Written by the Kafka consumer thread, read by the frame broadcaster.
 */
@Component
public class FleetState {

    private final Map<String, RobotView> robots = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();

    public void apply(RobotTelemetry t) {
        boolean[] changed = {false};
        robots.compute(t.robotId(), (id, old) -> {
            // Telemetry and status come from different Kafka topics, so their relative order is
            // not guaranteed. Telemetry taken before the robot went OFFLINE must not revive it.
            if (old != null && old.status() == RobotStatus.OFFLINE && t.ts() <= old.statusTs()) {
                return old;
            }
            changed[0] = true;
            return RobotView.of(t);
        });
        if (changed[0]) {
            dirty.add(t.robotId());
        }
    }

    public void apply(RobotStatusMessage s) {
        robots.compute(s.robotId(), (id, old) -> old == null ? RobotView.of(s) : old.withStatus(s));
        dirty.add(s.robotId());
    }

    /** Every robot, sorted by ID. */
    public List<RobotView> snapshot() {
        return robots.values().stream().sorted(Comparator.comparing(RobotView::id)).toList();
    }

    /**
     * Robots changed since the previous call, sorted by ID, and clears the change set.
     * A robot updated while this runs may be reported now and again next time; deltas carry
     * whole robots, so a repeat is harmless.
     */
    public List<RobotView> drainChanges() {
        List<RobotView> changed = new ArrayList<>();
        for (Iterator<String> it = dirty.iterator(); it.hasNext(); ) {
            String id = it.next();
            it.remove();
            RobotView view = robots.get(id);
            if (view != null) {
                changed.add(view);
            }
        }
        changed.sort(Comparator.comparing(RobotView::id));
        return changed;
    }
}
