package com.warehouse.bridge;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.warehouse.common.Topics;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Holds the bridge's MQTT connection and hands every message to {@link KafkaForwarder}.
 *
 * <ul>
 *   <li>Telemetry uses a shared subscription, so replicas split the stream instead of each
 *       forwarding every message.</li>
 *   <li>Status uses a normal subscription: MQTT 5 does not deliver retained messages to shared
 *       subscriptions, and the retained status is how the bridge learns the current state of
 *       every robot. Each replica forwards it, which is harmless on a compacted topic.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(prefix = "bridge", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MqttSubscriber implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MqttSubscriber.class);

    private final BridgeProperties props;
    private final Mqtt5AsyncClient client;
    private final String telemetryFilter;
    private final String statusFilter;
    private volatile boolean running;

    public MqttSubscriber(BridgeProperties props, KafkaForwarder forwarder) {
        this.props = props;
        this.telemetryFilter = "$share/%s/%s".formatted(props.shareGroup(), Topics.allRobotTelemetry(Topics.ANY_SITE));
        this.statusFilter = Topics.allRobotStatus(Topics.ANY_SITE);
        this.client = MqttClient.builder()
                .useMqttVersion5()
                .identifier(props.clientId())
                .serverHost(props.mqtt().host())
                .serverPort(props.mqtt().port())
                .automaticReconnectWithDefaultConfig()
                // Clean sessions forget subscriptions, so subscribe on every (re)connect.
                .addConnectedListener(ctx -> {
                    log.info("Connected to mqtt://{}:{} as {}", props.mqtt().host(), props.mqtt().port(), props.clientId());
                    subscribe();
                })
                .addDisconnectedListener(ctx -> log.info("MQTT disconnected: {}", ctx.getCause().toString()))
                .buildAsync();
        // One callback for all subscriptions, registered once (per-subscribe callbacks would pile up on resubscribe).
        client.publishes(MqttGlobalPublishFilter.ALL,
                publish -> forwarder.forward(publish.getTopic().toString(), publish.getPayloadAsBytes()));
    }

    /** Automatic reconnect also covers a failed first connect, so the broker may start after us. */
    @Override
    public void start() {
        running = true;
        client.connectWith()
                // Clean session, expiring on disconnect. A persistent session would stay in the
                // telemetry share group while we are down, and the broker would keep routing a
                // share of the QoS 0 telemetry to it, where it is dropped. Missed status changes
                // are not lost for good: the retained status arrives again on resubscribe.
                .cleanStart(true)
                .keepAlive(10)
                .send()
                .whenComplete((ack, error) -> {
                    if (error != null) {
                        log.warn("Cannot reach mqtt://{}:{} yet ({}); retrying in the background",
                                props.mqtt().host(), props.mqtt().port(), error.toString());
                    }
                });
    }

    private void subscribe() {
        client.subscribeWith()
                .addSubscription().topicFilter(telemetryFilter).qos(MqttQos.AT_MOST_ONCE).applySubscription()
                .addSubscription().topicFilter(statusFilter).qos(MqttQos.AT_LEAST_ONCE).applySubscription()
                .send()
                .whenComplete((ack, error) -> {
                    if (error != null) {
                        log.error("Subscribe failed", error);
                    } else {
                        log.info("Subscribed to {} and {}: {}", telemetryFilter, statusFilter, ack.getReasonCodes());
                    }
                });
    }

    @Override
    public void stop() {
        running = false;
        try {
            client.disconnect().get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.debug("Disconnect: {}", e.toString());
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Stop before the Kafka producer is closed, so in-flight messages still have somewhere to go. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}
