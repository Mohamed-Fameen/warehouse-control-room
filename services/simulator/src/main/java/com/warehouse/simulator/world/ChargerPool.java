package com.warehouse.simulator.world;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Charging spots shared by the fleet. Each one holds at most one robot. Thread-safe. */
public final class ChargerPool {

    private final List<Cell> chargers;
    private final Set<Cell> taken = new HashSet<>();

    public ChargerPool(List<Cell> chargers) {
        this.chargers = List.copyOf(chargers);
    }

    /** Reserves the free charger closest to {@code near}, if any. */
    public synchronized Optional<Cell> reserveNearest(Cell near) {
        Optional<Cell> best = chargers.stream()
                .filter(c -> !taken.contains(c))
                .min(Comparator.comparingInt(near::manhattan));
        best.ifPresent(taken::add);
        return best;
    }

    /** Reserves a specific charger; false if it is already taken. */
    public synchronized boolean reserve(Cell charger) {
        if (!chargers.contains(charger)) {
            throw new IllegalArgumentException("not a charger: " + charger);
        }
        return taken.add(charger);
    }

    public synchronized void release(Cell charger) {
        taken.remove(charger);
    }

    public int size() {
        return chargers.size();
    }
}
