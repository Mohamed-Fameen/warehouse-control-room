package com.warehouse.simulator.world;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class WarehouseGridTest {

    private final WarehouseGrid grid = WarehouseGrid.standard();

    @Test
    void specialCellsAreWalkable() {
        assertThat(grid.chargers()).hasSize(37).allMatch(c -> grid.isWalkable(c.x(), c.y()));
        assertThat(grid.packStations()).isNotEmpty().allMatch(c -> grid.isWalkable(c.x(), c.y()));
        assertThat(grid.pickSpots()).isNotEmpty().allMatch(c -> grid.isWalkable(c.x(), c.y()));
    }

    @Test
    void pathIsContiguousAndAvoidsShelves() {
        Cell from = grid.chargers().getFirst();
        Cell to = grid.packStations().getLast();
        List<Cell> path = grid.path(from, to);

        assertThat(path).isNotEmpty().endsWith(to).doesNotContain(from);
        Cell previous = from;
        for (Cell cell : path) {
            assertThat(previous.manhattan(cell)).isEqualTo(1);
            assertThat(grid.isWalkable(cell.x(), cell.y())).isTrue();
            previous = cell;
        }
        assertThat(path).hasSize(from.manhattan(to)); // the open cross-aisles allow a shortest path
    }

    @Test
    void everyPickSpotIsReachable() {
        Cell start = grid.chargers().getFirst();
        for (Cell spot : grid.pickSpots()) {
            assertThat(grid.path(start, spot)).isNotNull();
        }
    }

    @Test
    void rejectsShelfCells() {
        assertThat(grid.isShelf(4, 4)).isTrue();
        assertThatThrownBy(() -> grid.path(new Cell(0, 0), new Cell(4, 4)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
