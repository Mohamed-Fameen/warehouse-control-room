package com.warehouse.gateway.orders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.gateway.GatewayProperties;
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
 * Pushes order board changes to every {@code /ws/orders} browser, batched per frame like the
 * fleet stream: a snapshot on join, then deltas holding only the orders that changed.
 */
@Component
public class OrderBroadcaster implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OrderBroadcaster.class);
    private static final int SEND_TIME_LIMIT_MS = 2_000;
    private static final int BUFFER_LIMIT_BYTES = 512 * 1024;

    /**
     * One {@code /ws/orders} frame.
     *
     * <ul>
     *   <li>{@code snapshot}: once on connect; the whole board, newest first. Replace local state.</li>
     *   <li>{@code delta}: changed orders (each complete; upsert by {@code id}) and the IDs of
     *       orders dropped from the board ({@code removed}).</li>
     * </ul>
     */
    public record OrderMessage(String type, long ts, List<OrderBoard.OrderCard> orders, List<String> removed) {
    }

    private final OrderBoard board;
    private final ObjectMapper json;
    private final GatewayProperties props;
    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();
    /** Joining and ticking hold this, so a new session never misses a change between its snapshot and its first delta. */
    private final Object frameLock = new Object();
    private ScheduledExecutorService scheduler;

    public OrderBroadcaster(OrderBoard board, ObjectMapper json, GatewayProperties props, MeterRegistry meters) {
        this.board = board;
        this.json = json;
        this.props = props;
        meters.gaugeCollectionSize("gateway.order.sessions", List.of(), sessions);
    }

    void join(WebSocketSession raw) throws IOException {
        WebSocketSession session = new ConcurrentWebSocketSessionDecorator(raw, SEND_TIME_LIMIT_MS, BUFFER_LIMIT_BYTES);
        synchronized (frameLock) {
            OrderMessage snapshot = new OrderMessage("snapshot", System.currentTimeMillis(), board.snapshot(), List.of());
            session.sendMessage(new TextMessage(toJson(snapshot)));
            sessions.add(session);
        }
    }

    void leave(WebSocketSession raw) {
        sessions.removeIf(s -> s.getId().equals(raw.getId()));
    }

    void tick() {
        try {
            synchronized (frameLock) {
                OrderBoard.Changes changes = board.drainChanges();
                if (changes.isEmpty() || sessions.isEmpty()) {
                    return;
                }
                TextMessage frame = new TextMessage(toJson(
                        new OrderMessage("delta", System.currentTimeMillis(), changes.orders(), changes.removed())));
                for (WebSocketSession session : sessions) {
                    send(session, frame);
                }
            }
        } catch (RuntimeException e) {
            log.error("Order frame failed", e); // never let the scheduler die
        }
    }

    private void send(WebSocketSession session, TextMessage frame) {
        try {
            session.sendMessage(frame);
        } catch (Exception e) {
            log.debug("Dropping order session {}: {}", session.getId(), e.toString());
            sessions.remove(session);
            try {
                session.close(CloseStatus.SESSION_NOT_RELIABLE);
            } catch (IOException ignored) {
                // already gone
            }
        }
    }

    private String toJson(OrderMessage message) {
        try {
            return json.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("order-frames").daemon().factory());
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
