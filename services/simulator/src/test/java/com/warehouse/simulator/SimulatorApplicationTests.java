package com.warehouse.simulator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "simulator.enabled=false")
class SimulatorApplicationTests {

    @Test
    void contextLoads() {
    }
}
