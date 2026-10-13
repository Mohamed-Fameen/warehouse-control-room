package com.warehouse.dispatcher;

import com.warehouse.dispatcher.board.JobBoard;
import com.warehouse.dispatcher.messaging.MqttLink;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Owns the {@link JobBoard} and runs everything that touches it on one thread: RabbitMQ
 * deliveries, MQTT messages and the periodic assignment tick. No locks, and events are applied
 * in the order they arrive.
 */
@Component
public class DispatchLoop implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DispatchLoop.class);

    private final ScheduledExecutorService thread =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("dispatch-loop").factory());
    private final JobBoard board;
    private final DispatcherProperties props;
    private volatile boolean running;

    public DispatchLoop(DispatcherProperties props, MqttLink mqtt) {
        this.props = props;
        this.board = new JobBoard(props.boardSettings(), mqtt::sendCommand);
        mqtt.onMessage(this::submit);
    }

    /** Runs {@code action} on the loop thread. */
    public void submit(Consumer<JobBoard> action) {
        thread.execute(() -> {
            try {
                action.accept(board);
            } catch (RuntimeException e) {
                log.error("Dispatch loop action failed", e);
            }
        });
    }

    public JobBoard.Snapshot snapshot() throws InterruptedException, ExecutionException, TimeoutException {
        return thread.submit(board::snapshot).get(2, TimeUnit.SECONDS);
    }

    @Override
    public void start() {
        long period = props.tick().toMillis();
        thread.scheduleAtFixedRate(() -> {
            try {
                board.tick(System.currentTimeMillis());
            } catch (RuntimeException e) {
                log.error("Dispatch tick failed", e);
            }
        }, period, period, TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        thread.shutdownNow();
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
