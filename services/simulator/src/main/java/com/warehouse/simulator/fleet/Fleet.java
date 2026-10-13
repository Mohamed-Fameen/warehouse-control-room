package com.warehouse.simulator.fleet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.warehouse.common.layout.Cell;
import com.warehouse.common.layout.WarehouseGrid;
import com.warehouse.simulator.SimulatorProperties;
import com.warehouse.simulator.world.ChargerPool;
import com.warehouse.simulator.world.Robot;
import com.warehouse.simulator.world.RobotSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/** Builds the robot fleet on startup and runs each robot on its own virtual thread. */
@Component
@ConditionalOnProperty(prefix = "simulator", name = "enabled", havingValue = "true", matchIfMissing = true)
public class Fleet implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Fleet.class);

    private final SimulatorProperties props;
    private final ObjectMapper json;
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;

    public Fleet(SimulatorProperties props, ObjectMapper json) {
        this.props = props;
        this.json = json;
    }

    @Override
    public synchronized void start() {
        WarehouseGrid grid = WarehouseGrid.standard();
        ChargerPool chargers = new ChargerPool(grid.chargers());
        if (props.robotCount() > chargers.size()) {
            throw new IllegalStateException("simulator.robot-count is %d but the layout has only %d chargers"
                    .formatted(props.robotCount(), chargers.size()));
        }
        RobotSpec spec = props.robot().toSpec();

        for (int i = 0; i < props.robotCount(); i++) {
            String id = "r-%02d".formatted(i + 1);
            RandomGenerator random = new SplittableRandom(props.seed() + i);
            Cell dock = grid.chargers().get(i);
            chargers.reserve(dock);
            double battery = 40 + random.nextDouble() * 60;
            Robot robot = new Robot(id, dock, battery, grid, chargers, spec, random);
            threads.add(Thread.ofVirtual().name("robot-" + id).start(new RobotRunner(robot, props, json)));
        }
        running = true;
        log.info("Started {} robots at site {} -> mqtt://{}:{}",
                props.robotCount(), props.site(), props.mqtt().host(), props.mqtt().port());
    }

    @Override
    public synchronized void stop() {
        threads.forEach(Thread::interrupt);
        for (Thread thread : threads) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        threads.clear();
        running = false;
        log.info("Fleet stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
