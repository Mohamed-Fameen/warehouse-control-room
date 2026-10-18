package com.warehouse.dispatcher;

import com.warehouse.dispatcher.board.JobBoard;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Settings under {@code dispatcher.*}; defaults live in application.yml. */
@ConfigurationProperties("dispatcher")
public record DispatcherProperties(
        String site,
        String clientId,
        Mqtt mqtt,
        Duration tick,
        Duration assignTimeout,
        Duration jobTimeout,
        double minBattery,
        Duration staleAfter,
        Duration rejectCooldown,
        Duration completedMemory,
        int maxAttempts) {

    public record Mqtt(String host, int port) {
    }

    public JobBoard.Settings boardSettings() {
        return new JobBoard.Settings(assignTimeout, jobTimeout, minBattery, staleAfter, rejectCooldown, completedMemory, maxAttempts);
    }
}
