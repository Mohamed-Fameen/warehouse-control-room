package com.warehouse.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.RobotStatus;
import com.warehouse.common.RobotTelemetry;
import com.warehouse.gateway.fleet.FleetState;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/** Web layer without Kafka: state is fed directly. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"gateway.kafka-enabled=false", "gateway.frame-interval=50ms"})
class GatewayApplicationTests {

    @LocalServerPort
    int port;

    @Autowired
    FleetState state;

    @Autowired
    TestRestTemplate http;

    @Autowired
    ObjectMapper json;

    @Test
    void servesLayout() throws Exception {
        JsonNode layout = json.readTree(http.getForObject("/api/layout", String.class));
        assertThat(layout.get("width").asInt()).isEqualTo(40);
        assertThat(layout.get("racks")).hasSize(16);
        assertThat(layout.get("chargers")).hasSize(37);
    }

    @Test
    void sendsSnapshotThenDeltas() throws Exception {
        state.apply(new RobotTelemetry("r-01", "dc1", 0, 1_000, 1.5, 1.5, 0, 0, 90, RobotStatus.IDLE));

        BlockingQueue<String> frames = new LinkedBlockingQueue<>();
        WebSocketSession session = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession s, TextMessage message) {
                frames.add(message.getPayload());
            }
        }, null, URI.create("ws://localhost:" + port + "/ws/fleet")).get(5, TimeUnit.SECONDS);

        try {
            JsonNode snapshot = json.readTree(frames.poll(5, TimeUnit.SECONDS));
            assertThat(snapshot.get("type").asText()).isEqualTo("snapshot");
            assertThat(snapshot.get("robots").findValuesAsText("id")).contains("r-01");

            state.apply(new RobotTelemetry("r-02", "dc1", 0, 1_000, 3.5, 1.5, 0, 1.5, 70, RobotStatus.MOVING));
            JsonNode delta = nextDeltaWith(frames, "r-02");
            JsonNode r02 = delta.get("robots").get(0);
            assertThat(r02.get("x").asDouble()).isEqualTo(3.5);
            assertThat(r02.get("status").asText()).isEqualTo("MOVING");
        } finally {
            session.close();
        }
    }

    private JsonNode nextDeltaWith(BlockingQueue<String> frames, String robotId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            String frame = frames.poll(1, TimeUnit.SECONDS);
            if (frame != null) {
                JsonNode node = json.readTree(frame);
                if (node.get("type").asText().equals("delta")
                        && node.get("robots").findValuesAsText("id").contains(robotId)) {
                    return node;
                }
            }
        }
        throw new AssertionError("no delta for " + robotId);
    }
}
