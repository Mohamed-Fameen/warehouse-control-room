package com.warehouse.common.layout;

/** Axis-aligned block of cells: {@code x..x+width-1} by {@code y..y+height-1}. */
public record Rect(int x, int y, int width, int height) {
}
