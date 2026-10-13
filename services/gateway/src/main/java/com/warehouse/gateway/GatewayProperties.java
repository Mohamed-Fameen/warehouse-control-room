package com.warehouse.gateway;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings under {@code gateway.*}; defaults live in application.yml.
 *
 * @param frameInterval  how often changed robots are pushed to browsers (100 ms = 10 fps)
 * @param allowedOrigins browser origins allowed to open the WebSocket and call the REST API
 */
@ConfigurationProperties("gateway")
public record GatewayProperties(
        boolean kafkaEnabled,
        Duration frameInterval,
        List<String> allowedOrigins) {
}
