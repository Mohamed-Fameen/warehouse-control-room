package com.warehouse.gateway.ws;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/** {@code /ws/fleet}: server-to-browser only; anything the browser sends is ignored. */
@Component
public class FleetWebSocketHandler extends TextWebSocketHandler {

    private final FleetBroadcaster broadcaster;

    public FleetWebSocketHandler(FleetBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        broadcaster.join(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        broadcaster.leave(session);
    }
}
