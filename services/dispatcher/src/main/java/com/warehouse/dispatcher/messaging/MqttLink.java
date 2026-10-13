package com.warehouse.dispatcher.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.warehouse.common.RobotCommand;
import com.warehouse.common.RobotJobUpdate;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.common.Topics;
import com.warehouse.dispatcher.DispatcherProperties;
import com.warehouse.dispatcher.board.JobBoard;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The dispatcher's MQTT connection. Reads every robot's telemetry, status and job progress
 * (normal subscriptions: each dispatcher replica needs the whole fleet, and the retained status
 * and job messages tell a fresh dispatcher where things stand), and sends commands.
 */
@Component
public class MqttLink implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MqttLink.class);

    /** Time allowed for retained status and job messages to arrive after subscribing. */
    private static final long RETAINED_GRACE_MS = 2_000;

    private final DispatcherProperties props;
    private final ObjectMapper json;
    private final Mqtt5AsyncClient client;
    private volatile Consumer<Consumer<JobBoard>> sink = action -> { };
    private volatile boolean running;

    public MqttLink(DispatcherProperties props, ObjectMapper json) {
        this.props = props;
        this.json = json;
        this.client = MqttClient.builder()
                .useMqttVersion5()
                .identifier(props.clientId())
                .serverHost(props.mqtt().host())
                .serverPort(props.mqtt().port())
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(ctx -> {
                    log.info("Connected to mqtt://{}:{} as {}", props.mqtt().host(), props.mqtt().port(), props.clientId());
                    subscribe(); // clean sessions forget subscriptions
                })
                .addDisconnectedListener(ctx -> log.info("MQTT disconnected: {}", ctx.getCause().toString()))
                .buildAsync();
        client.publishes(MqttGlobalPublishFilter.SUBSCRIBED, this::receive);
    }

    /** Where parsed messages go, as actions on the board. Set once by the dispatch loop. */
    public void onMessage(Consumer<Consumer<JobBoard>> sink) {
        this.sink = sink;
    }

    /** QoS 1, not retained: a command must not be replayed to a robot that reconnects later. */
    public void sendCommand(String robotId, RobotCommand command) {
        if (!client.getState().isConnected()) {
            return; // the assignment times out and the job goes to another robot
        }
        try {
            client.publishWith()
                    .topic(Topics.robotCommand(props.site(), robotId))
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload(json.writeValueAsBytes(command))
                    .send();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Runs on an MQTT thread: parse, then hand over to the dispatch loop. */
    private void receive(Mqtt5Publish publish) {
        // wh/{site}/robot/{id}/{kind}
        List<String> levels = publish.getTopic().getLevels();
        if (levels.size() != 5) {
            return;
        }
        String robotId = levels.get(3);
        byte[] payload = publish.getPayloadAsBytes();
        try {
            switch (levels.get(4)) {
                case "telemetry" -> {
                    RobotTelemetry t = json.readValue(payload, RobotTelemetry.class);
                    sink.accept(board -> board.onTelemetry(t, System.currentTimeMillis()));
                }
                case "status" -> {
                    RobotStatusMessage s = json.readValue(payload, RobotStatusMessage.class);
                    sink.accept(board -> board.onStatus(s, System.currentTimeMillis()));
                }
                case "job" -> {
                    RobotJobUpdate u = payload.length == 0 ? null : json.readValue(payload, RobotJobUpdate.class);
                    sink.accept(board -> board.onJobUpdate(robotId, u, System.currentTimeMillis()));
                }
                default -> {
                }
            }
        } catch (IOException e) {
            log.warn("Skipping unreadable message on {}: {}", publish.getTopic(), e.getMessage());
        }
    }

    private void subscribe() {
        String site = props.site();
        client.subscribeWith()
                .addSubscription().topicFilter(Topics.allRobotTelemetry(site)).qos(MqttQos.AT_MOST_ONCE).applySubscription()
                .addSubscription().topicFilter(Topics.allRobotStatus(site)).qos(MqttQos.AT_LEAST_ONCE).applySubscription()
                .addSubscription().topicFilter(Topics.allRobotJobs(site)).qos(MqttQos.AT_LEAST_ONCE).applySubscription()
                .send()
                .whenComplete((ack, error) -> {
                    if (error != null) {
                        log.error("Subscribe failed", error);
                    } else {
                        log.info("Subscribed to robot telemetry, status and jobs at {}: {}", site, ack.getReasonCodes());
                        long ready = System.currentTimeMillis() + RETAINED_GRACE_MS;
                        sink.accept(board -> board.holdAssignmentsUntil(ready));
                    }
                });
    }

    /** Automatic reconnect also covers a failed first connect, so the broker may start after us. */
    @Override
    public void start() {
        running = true;
        client.connectWith()
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
}
