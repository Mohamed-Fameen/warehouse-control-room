package com.warehouse.simulator.fleet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.common.Topics;
import com.warehouse.simulator.SimulatorProperties;
import com.warehouse.simulator.world.Robot;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives one robot on its own virtual thread with its own MQTT connection, so each robot has
 * its own last will: if the connection drops without a clean disconnect, the broker publishes
 * a retained {@code OFFLINE} status for that robot.
 */
final class RobotRunner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RobotRunner.class);
    private static final double MAX_STEP_SECONDS = 1.0;

    private final Robot robot;
    private final String site;
    private final Duration tick;
    private final long telemetryIntervalNanos;
    private final ObjectMapper json;
    private final Mqtt5AsyncClient client;
    private final String telemetryTopic;
    private final String statusTopic;

    /** Set on every (re)connect so the retained status is republished. */
    private volatile boolean statusDirty = true;
    private long seq;

    RobotRunner(Robot robot, SimulatorProperties props, ObjectMapper json) {
        this.robot = robot;
        this.site = props.site();
        this.tick = props.tick();
        this.telemetryIntervalNanos = props.telemetryInterval().toNanos();
        this.json = json;
        this.telemetryTopic = Topics.robotTelemetry(site, robot.id());
        this.statusTopic = Topics.robotStatus(site, robot.id());
        this.client = MqttClient.builder()
                .useMqttVersion5()
                .identifier("sim-%s-%s".formatted(site, robot.id()))
                .serverHost(props.mqtt().host())
                .serverPort(props.mqtt().port())
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(ctx -> statusDirty = true)
                .willPublish()
                    .topic(statusTopic)
                    .payload(toJson(new RobotStatusMessage(robot.id(), RobotStatus.OFFLINE, 0)))
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .retain(true)
                    .applyWillPublish()
                .buildAsync();
    }

    @Override
    public void run() {
        try {
            connect();
            RobotStatus lastStatus = robot.status();
            long last = System.nanoTime();
            long nextTelemetry = last;
            while (true) {
                Thread.sleep(tick);
                long now = System.nanoTime();
                robot.step(Math.min((now - last) / 1e9, MAX_STEP_SECONDS));
                last = now;

                RobotStatus status = robot.status();
                if (status != lastStatus || statusDirty) {
                    publishStatus(status);
                    lastStatus = status;
                }
                if (now - nextTelemetry >= 0) {
                    publishTelemetry(status);
                    nextTelemetry += telemetryIntervalNanos;
                    if (now - nextTelemetry >= 0) {
                        nextTelemetry = now + telemetryIntervalNanos; // fell behind: skip, do not burst
                    }
                }
            }
        } catch (InterruptedException e) {
            // Normal shutdown.
        } finally {
            goOffline();
        }
    }

    /**
     * The first connect is retried here; after that the client's automatic reconnect takes over.
     * Telemetry is dropped, not buffered, while disconnected.
     */
    private void connect() throws InterruptedException {
        long backoffMs = 500;
        while (true) {
            try {
                client.connectWith()
                        .cleanStart(true)
                        .keepAlive(10)
                        .send()
                        .get(10, TimeUnit.SECONDS);
                log.debug("{} connected", robot.id());
                return;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                log.warn("{} cannot connect to MQTT ({}), retrying in {} ms",
                        robot.id(), rootMessage(e), backoffMs);
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 10_000);
            }
        }
    }

    private void publishTelemetry(RobotStatus status) {
        if (!client.getState().isConnected()) {
            return;
        }
        RobotTelemetry telemetry = new RobotTelemetry(
                robot.id(), site, seq++, System.currentTimeMillis(),
                round(robot.x()), round(robot.y()), round(robot.heading()),
                round(robot.speed()), round(robot.battery()), status);
        client.publishWith()
                .topic(telemetryTopic)
                .qos(MqttQos.AT_MOST_ONCE)
                .payload(toJson(telemetry))
                .send();
    }

    private void publishStatus(RobotStatus status) {
        if (!client.getState().isConnected()) {
            return;
        }
        statusDirty = false;
        client.publishWith()
                .topic(statusTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .retain(true)
                .payload(toJson(new RobotStatusMessage(robot.id(), status, System.currentTimeMillis())))
                .send()
                .whenComplete((result, error) -> {
                    if (error != null) {
                        statusDirty = true;
                        log.debug("{} status publish failed: {}", robot.id(), error.toString());
                    }
                });
    }

    /** A clean disconnect suppresses the last will, so publish OFFLINE ourselves first. */
    private void goOffline() {
        try {
            if (client.getState().isConnected()) {
                client.publishWith()
                        .topic(statusTopic)
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .retain(true)
                        .payload(toJson(new RobotStatusMessage(
                                robot.id(), RobotStatus.OFFLINE, System.currentTimeMillis())))
                        .send()
                        .get(2, TimeUnit.SECONDS);
            }
            client.disconnect().get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.debug("{} offline/disconnect: {}", robot.id(), e.toString());
        }
    }

    private byte[] toJson(Object value) {
        try {
            return json.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static double round(double v) {
        return Math.round(v * 100) / 100.0;
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
    }
}
