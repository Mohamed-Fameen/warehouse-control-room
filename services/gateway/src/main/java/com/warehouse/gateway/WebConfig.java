package com.warehouse.gateway;

import com.warehouse.gateway.orders.OrdersWebSocketHandler;
import com.warehouse.gateway.ws.FleetWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebConfig implements WebSocketConfigurer, WebMvcConfigurer {

    private final FleetWebSocketHandler fleetHandler;
    private final OrdersWebSocketHandler ordersHandler;
    private final GatewayProperties props;

    public WebConfig(FleetWebSocketHandler fleetHandler, OrdersWebSocketHandler ordersHandler, GatewayProperties props) {
        this.fleetHandler = fleetHandler;
        this.ordersHandler = ordersHandler;
        this.props = props;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        String[] origins = props.allowedOrigins().toArray(String[]::new);
        registry.addHandler(fleetHandler, "/ws/fleet").setAllowedOriginPatterns(origins);
        registry.addHandler(ordersHandler, "/ws/orders").setAllowedOriginPatterns(origins);
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOriginPatterns(props.allowedOrigins().toArray(String[]::new))
                .allowedMethods("GET");
    }
}
