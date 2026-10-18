package com.warehouse.gateway.orders;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/** {@code /ws/orders}: server-to-browser only; anything the browser sends is ignored. */
@Component
public class OrdersWebSocketHandler extends TextWebSocketHandler {

    private final OrderBroadcaster broadcaster;

    public OrdersWebSocketHandler(OrderBroadcaster broadcaster) {
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
