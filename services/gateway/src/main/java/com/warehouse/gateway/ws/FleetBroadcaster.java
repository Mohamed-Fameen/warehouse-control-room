package com.warehouse.gateway.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.gateway.GatewayProperties;
import com.warehouse.gateway.fleet.FleetState;
import com.warehouse.gateway.fleet.RobotView;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

/**
 * Pushes robot changes to every connected browser, at most once per frame interval. Robots
 * report at 5 Hz each, so without this a browser would get one message per robot update;
 * instead it gets one batched delta per frame.
 */
@Component
public class FleetBroadcaster implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(FleetBroadcaster.class);
    /** A browser that cannot take a frame within this time is disconnected; it reconnects and gets a snapshot. */
    private static final int SEND_TIME_LIMIT_MS = 2_000;
    private static final int BUFFER_LIMIT_BYTES = 512 * 1024;

    private final FleetState state;
    private final ObjectMapper json;
    private final GatewayProperties props;
    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();
    /** Joining and ticking hold this, so a new session never misses a change between its snapshot and its first delta. */
    private final Object frameLock = new Object();
    private ScheduledExecutorService scheduler;

    public FleetBroadcaster(FleetState state, ObjectMapper json, GatewayProperties props, MeterRegistry meters) {
        this.state = state;
        this.json = json;
        this.props = props;
        meters.gaugeCollectionSize("gateway.sessions", List.of(), sessions);
    }

    /** Sends the snapshot and adds the session to the broadcast list. */
    void join(WebSocketSession raw) throws IOException {
        WebSocketSession session = new ConcurrentWebSocketSessionDecorator(raw, SEND_TIME_LIMIT_MS, BUFFER_LIMIT_BYTES);
        synchronized (frameLock) {
            session.sendMessage(new TextMessage(toJson(FleetMessage.snapshot(state.snapshot()))));
            sessions.add(session);
        }
    }

    void leave(WebSocketSession raw) {
        sessions.removeIf(s -> s.getId().equals(raw.getId()));
    }

    void tick() {
        try {
            synchronized (frameLock) {
                List<RobotView> changed = state.drainChanges();
                if (changed.isEmpty() || sessions.isEmpty()) {
                    return;
                }
                TextMessage frame = new TextMessage(toJson(FleetMessage.delta(changed)));
                for (WebSocketSession session : sessions) {
                    send(session, frame);
                }
            }
        } catch (RuntimeException e) {
            log.error("Frame failed", e); // never let the scheduler die
        }
    }

    private void send(WebSocketSession session, TextMessage frame) {
        try {
            session.sendMessage(frame);
        } catch (Exception e) {
            log.debug("Dropping session {}: {}", session.getId(), e.toString());
            sessions.remove(session);
            try {
                session.close(CloseStatus.SESSION_NOT_RELIABLE);
            } catch (IOException ignored) {
                // already gone
            }
        }
    }

    private String toJson(FleetMessage message) {
        try {
            return json.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("fleet-frames").daemon().factory());
        long ms = props.frameInterval().toMillis();
        scheduler.scheduleAtFixedRate(this::tick, ms, ms, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        scheduler.shutdownNow();
        scheduler = null;
    }

    @Override
    public boolean isRunning() {
        return scheduler != null;
    }
}
