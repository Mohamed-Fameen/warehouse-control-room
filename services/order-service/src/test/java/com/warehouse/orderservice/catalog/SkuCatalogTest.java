package com.warehouse.orderservice.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import com.warehouse.common.layout.WarehouseGrid;
import org.junit.jupiter.api.Test;

class SkuCatalogTest {

    private final SkuCatalog catalog = new SkuCatalog();
    private final WarehouseGrid grid = WarehouseGrid.standard();

    @Test
    void everySkuMapsToAPickSpot() {
        assertThat(catalog.size()).isEqualTo(grid.pickSpots().size());
        assertThat(catalog.location(catalog.sku(0))).contains(grid.pickSpots().getFirst());
        assertThat(catalog.location(catalog.sku(catalog.size() - 1))).contains(grid.pickSpots().getLast());
    }

    @Test
    void rejectsUnknownSkus() {
        assertThat(catalog.location("SKU-0000")).isEmpty();
        assertThat(catalog.location(catalog.sku(catalog.size()))).isEmpty();
        assertThat(catalog.location("sku-0001")).isEmpty();
        assertThat(catalog.location(null)).isEmpty();
    }
}
