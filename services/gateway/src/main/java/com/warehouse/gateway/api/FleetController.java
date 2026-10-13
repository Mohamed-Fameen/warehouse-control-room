package com.warehouse.gateway.api;

import com.warehouse.common.layout.Cell;
import com.warehouse.common.layout.Rect;
import com.warehouse.common.layout.WarehouseGrid;
import com.warehouse.gateway.fleet.FleetState;
import com.warehouse.gateway.fleet.RobotView;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class FleetController {

    /**
     * Static floor plan for drawing the warehouse. Units are metres (one cell = 1 m); a cell's
     * centre is at {@code (x + 0.5, y + 0.5)}; racks cover {@code x..x+width} by {@code y..y+height}.
     */
    public record LayoutView(int width, int height, List<Rect> racks, List<Cell> chargers, List<Cell> packStations) {
    }

    private static final WarehouseGrid GRID = WarehouseGrid.standard();
    private static final LayoutView LAYOUT =
            new LayoutView(GRID.width(), GRID.height(), GRID.racks(), GRID.chargers(), GRID.packStations());

    private final FleetState state;

    public FleetController(FleetState state) {
        this.state = state;
    }

    @GetMapping("/layout")
    public LayoutView layout() {
        return LAYOUT;
    }

    /** Same content as the WebSocket snapshot; handy for curl and debugging. */
    @GetMapping("/robots")
    public List<RobotView> robots() {
        return state.snapshot();
    }
}
