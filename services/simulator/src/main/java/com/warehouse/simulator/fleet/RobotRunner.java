package com.warehouse.simulator.fleet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.warehouse.common.RobotCommand;
import com.warehouse.common.RobotCommandType;
import com.warehouse.common.RobotJobState;
import com.warehouse.common.RobotJobUpdate;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotStatusMessage;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.common.Topics;
import com.warehouse.simulator.SimulatorProperties;
import com.warehouse.simulator.world.Robot;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives one robot on its own virtual thread with its own MQTT connection, so each robot has
 * its own last will: if the connection drops without a clean disconnect, the broker publishes
 * a retained {@code OFFLINE} status for that robot.
 *
 * <p>Commands arrive on MQTT threads and wait in an inbox until the robot's own thread applies
 * them between steps, so {@link Robot} stays single-threaded.
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
    private final String commandTopic;
    private final String jobTopic;
    private final Queue<RobotCommand> inbox = new ConcurrentLinkedQueue<>();

    /** Set on every (re)connect so the retained status is republished. */
    private volatile boolean statusDirty = true;
    /** Set on every (re)connect so the retained job state is republished (or cleared). */
    private volatile boolean jobDirty = true;
    private long seq;

    RobotRunner(Robot robot, SimulatorProperties props, ObjectMapper json) {
        this.robot = robot;
        this.site = props.site();
        this.tick = props.tick();
        this.telemetryIntervalNanos = props.telemetryInterval().toNanos();
        this.json = json;
        this.telemetryTopic = Topics.robotTelemetry(site, robot.id());
        this.statusTopic = Topics.robotStatus(site, robot.id());
        this.commandTopic = Topics.robotCommand(site, robot.id());
        this.jobTopic = Topics.robotJob(site, robot.id());
        this.client = MqttClient.builder()
                .useMqttVersion5()
                .identifier("sim-%s-%s".formatted(site, robot.id()))
                .serverHost(props.mqtt().host())
                .serverPort(props.mqtt().port())
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(ctx -> {
                    statusDirty = true;
                    jobDirty = true;
                    subscribeToCommands(); // clean sessions forget subscriptions
                })
                .willPublish()
                    .topic(statusTopic)
                    .payload(toJson(new RobotStatusMessage(robot.id(), RobotStatus.OFFLINE, 0)))
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .retain(true)
                    .applyWillPublish()
                .buildAsync();
        client.publishes(MqttGlobalPublishFilter.SUBSCRIBED, publish -> receive(publish.getPayloadAsBytes()));
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
                applyCommands();
                robot.step(Math.min((now - last) / 1e9, MAX_STEP_SECONDS));
                last = now;

                publishJobEvents();

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
     * Starts connecting without waiting. Automatic reconnect also covers a failed first connect,
     * and nothing is published until the client is connected. Telemetry is dropped, not buffered,
     * while disconnected.
     */
    private void connect() {
        client.connectWith()
                .cleanStart(true)
                .keepAlive(10)
                .send()
                .whenComplete((ack, error) -> {
                    if (error != null) {
                        log.warn("{} cannot reach MQTT yet ({}); retrying in the background",
                                robot.id(), rootMessage(error));
                    }
                });
    }

    private void subscribeToCommands() {
        client.subscribeWith()
                .topicFilter(commandTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send()
                .whenComplete((ack, error) -> {
                    if (error != null) {
                        log.warn("{} cannot subscribe to commands: {}", robot.id(), rootMessage(error));
                    }
                });
    }

    /** Runs on an MQTT thread: parse and queue only. */
    private void receive(byte[] payload) {
        try {
            RobotCommand command = json.readValue(payload, RobotCommand.class);
            if (command.type() == RobotCommandType.ASSIGN_JOB && command.jobId() != null && command.pick() != null) {
                inbox.add(command);
            } else {
                log.warn("{} ignoring command {}", robot.id(), command);
            }
        } catch (IOException e) {
            log.warn("{} ignoring unreadable command: {}", robot.id(), e.getMessage());
        }
    }

    private void applyCommands() {
        RobotCommand command;
        while ((command = inbox.poll()) != null) {
            robot.assign(command.jobId(), command.pick());
        }
    }

    /**
     * Publishes job changes. All but rejections are retained, so the retained message is the
     * robot's latest job. After a (re)connect the current job is republished, or the retained
     * message is cleared when there is none, so a restarted robot never claims a job it lost.
     */
    private void publishJobEvents() {
        for (Robot.JobEvent event : robot.drainJobEvents()) {
            publishJob(event, event.state() != RobotJobState.REJECTED);
        }
        if (jobDirty && client.getState().isConnected()) {
            jobDirty = false;
            Optional<Robot.JobEvent> current = robot.currentJob();
            if (current.isPresent()) {
                publishJob(current.get(), true);
            } else {
                sendJob(new byte[0], true);
            }
        }
    }

    private void publishJob(Robot.JobEvent event, boolean retain) {
        RobotJobUpdate update = new RobotJobUpdate(
                robot.id(), event.jobId(), event.state(), event.reason(), System.currentTimeMillis());
        sendJob(toJson(update), retain);
    }

    /** QoS 1. A failed publish (for example while disconnected) republishes the current job state later. */
    private void sendJob(byte[] payload, boolean retain) {
        if (!client.getState().isConnected()) {
            jobDirty = true;
            return;
        }
        client.publishWith()
                .topic(jobTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .retain(retain)
                .payload(payload)
                .send()
                .whenComplete((result, error) -> {
                    if (error != null) {
                        jobDirty = true;
                        log.debug("{} job publish failed: {}", robot.id(), error.toString());
                    }
                });
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
