package com.warehouse.common.layout;

/** One square metre of warehouse floor. The robot stands at the cell centre. */
public record Cell(int x, int y) {

    public double centerX() {
        return x + 0.5;
    }

    public double centerY() {
        return y + 0.5;
    }

    public int manhattan(Cell other) {
        return Math.abs(x - other.x) + Math.abs(y - other.y);
    }
}
