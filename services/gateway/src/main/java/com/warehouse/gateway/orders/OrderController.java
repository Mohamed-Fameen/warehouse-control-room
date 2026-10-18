package com.warehouse.gateway.orders;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private final OrderBoard board;

    public OrderController(OrderBoard board) {
        this.board = board;
    }

    /** Same content as the {@code /ws/orders} snapshot; handy for curl and debugging. */
    @GetMapping("/api/orders")
    public List<OrderBoard.OrderCard> orders() {
        return board.snapshot();
    }
}
