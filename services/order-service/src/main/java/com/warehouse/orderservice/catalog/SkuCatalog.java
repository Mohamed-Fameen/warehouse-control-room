package com.warehouse.orderservice.catalog;

import com.warehouse.common.layout.Cell;
import com.warehouse.common.layout.WarehouseGrid;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * One SKU per pick spot: {@code SKU-0001} is stored at the first pick spot of the layout,
 * {@code SKU-0002} at the second, and so on. Stable as long as the layout does not change.
 */
@Component
public class SkuCatalog {

    private final List<Cell> spots = WarehouseGrid.standard().pickSpots();

    public int size() {
        return spots.size();
    }

    public String sku(int index) {
        return "SKU-%04d".formatted(index + 1);
    }

    /** Where to stop to pick this SKU, or empty if it does not exist. */
    public Optional<Cell> location(String sku) {
        if (sku == null || !sku.matches("SKU-\\d{4}")) {
            return Optional.empty();
        }
        int index = Integer.parseInt(sku.substring(4)) - 1;
        return index >= 0 && index < spots.size() ? Optional.of(spots.get(index)) : Optional.empty();
    }
}
