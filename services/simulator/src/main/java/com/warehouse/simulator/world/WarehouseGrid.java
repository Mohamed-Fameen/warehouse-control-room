package com.warehouse.simulator.world;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Floor plan on a 1 m grid. Shelves are blocked cells; everything else is aisle.
 * Robots move between cells with 4-way BFS paths.
 *
 * <pre>
 *  y=25 +----------------------------------------+
 *       |    P     P     P     P     P     P     |  pack stations (y=24)
 *       |                                        |  cross aisle
 *       |c   ##  ##  ##  ##  ##  ##  ##  ##     c|  shelf band y=14..21
 *       |    ##  ##  ##  ##  ##  ##  ##  ##      |
 *       |                                        |  cross aisle y=12..13
 *       |c   ##  ##  ##  ##  ##  ##  ##  ##     c|  shelf band y=4..11
 *       |    ##  ##  ##  ##  ##  ##  ##  ##      |
 *       |  c c c c c c c c c c c c c c c c c c c |  chargers (y=1)
 *  y=0  +----------------------------------------+ x=39
 * </pre>
 */
public final class WarehouseGrid {

    private final int width;
    private final int height;
    private final boolean[][] shelf;
    private final List<Cell> chargers;
    private final List<Cell> packStations;
    private final List<Cell> pickSpots;

    private WarehouseGrid(int width, int height, boolean[][] shelf,
                          List<Cell> chargers, List<Cell> packStations) {
        this.width = width;
        this.height = height;
        this.shelf = shelf;
        this.chargers = List.copyOf(chargers);
        this.packStations = List.copyOf(packStations);
        this.pickSpots = List.copyOf(findPickSpots());
    }

    /** The default 40 x 26 m layout drawn in the class comment. */
    public static WarehouseGrid standard() {
        int width = 40;
        int height = 26;
        boolean[][] shelf = new boolean[width][height];
        int[][] bands = {{4, 11}, {14, 21}};
        for (int x0 = 4; x0 + 1 < width - 4; x0 += 4) {
            for (int[] band : bands) {
                for (int y = band[0]; y <= band[1]; y++) {
                    shelf[x0][y] = true;
                    shelf[x0 + 1][y] = true;
                }
            }
        }

        List<Cell> chargers = new ArrayList<>();
        for (int x = 2; x < width - 1; x += 2) {
            chargers.add(new Cell(x, 1));
        }
        for (int y = 4; y <= 21; y += 2) {
            chargers.add(new Cell(1, y));
            chargers.add(new Cell(width - 2, y));
        }

        List<Cell> packStations = new ArrayList<>();
        for (int x = 4; x < width; x += 6) {
            packStations.add(new Cell(x, height - 2));
        }
        return new WarehouseGrid(width, height, shelf, chargers, packStations);
    }

    public int width() {
        return width;
    }

    public int height() {
        return height;
    }

    public boolean isShelf(int x, int y) {
        return inBounds(x, y) && shelf[x][y];
    }

    public boolean isWalkable(int x, int y) {
        return inBounds(x, y) && !shelf[x][y];
    }

    public List<Cell> chargers() {
        return chargers;
    }

    public List<Cell> packStations() {
        return packStations;
    }

    /** Aisle cells next to a shelf, where a robot stops to pick. */
    public List<Cell> pickSpots() {
        return pickSpots;
    }

    /**
     * Shortest 4-way path from {@code from} to {@code to}, excluding {@code from} and including
     * {@code to}. Empty when they are the same cell.
     *
     * @throws IllegalArgumentException if either end is not walkable or there is no path
     */
    public List<Cell> path(Cell from, Cell to) {
        if (!isWalkable(from.x(), from.y()) || !isWalkable(to.x(), to.y())) {
            throw new IllegalArgumentException("not walkable: " + from + " -> " + to);
        }
        if (from.equals(to)) {
            return List.of();
        }
        Cell[][] previous = new Cell[width][height];
        ArrayDeque<Cell> queue = new ArrayDeque<>();
        queue.add(from);
        previous[from.x()][from.y()] = from;
        int[][] steps = {{1, 0}, {-1, 0}, {0, 1}, {0, -1}};
        while (!queue.isEmpty()) {
            Cell cell = queue.poll();
            if (cell.equals(to)) {
                break;
            }
            for (int[] step : steps) {
                int nx = cell.x() + step[0];
                int ny = cell.y() + step[1];
                if (isWalkable(nx, ny) && previous[nx][ny] == null) {
                    previous[nx][ny] = cell;
                    queue.add(new Cell(nx, ny));
                }
            }
        }
        if (previous[to.x()][to.y()] == null) {
            throw new IllegalArgumentException("no path: " + from + " -> " + to);
        }
        List<Cell> path = new ArrayList<>();
        for (Cell c = to; !c.equals(from); c = previous[c.x()][c.y()]) {
            path.add(c);
        }
        Collections.reverse(path);
        return path;
    }

    private boolean inBounds(int x, int y) {
        return x >= 0 && y >= 0 && x < width && y < height;
    }

    private List<Cell> findPickSpots() {
        List<Cell> spots = new ArrayList<>();
        for (int x = 0; x < width; x++) {
            for (int y = 0; y < height; y++) {
                if (isWalkable(x, y) && (isShelf(x - 1, y) || isShelf(x + 1, y))) {
                    spots.add(new Cell(x, y));
                }
            }
        }
        return spots;
    }
}
