package com.warehouse.bridge;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "bridge.enabled=false")
class BridgeApplicationTests {

    @Test
    void contextLoads() {
    }
}
