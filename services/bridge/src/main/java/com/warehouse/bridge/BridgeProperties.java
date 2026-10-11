package com.warehouse.bridge;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings under {@code bridge.*}; defaults live in application.yml.
 *
 * @param clientId   MQTT client ID; must be unique per replica
 * @param shareGroup MQTT 5 shared-subscription group for telemetry; replicas in the same
 *                   group split the telemetry stream between them
 */
@ConfigurationProperties("bridge")
public record BridgeProperties(
        boolean enabled,
        String clientId,
        String shareGroup,
        Mqtt mqtt) {

    public record Mqtt(String host, int port) {
    }
}
