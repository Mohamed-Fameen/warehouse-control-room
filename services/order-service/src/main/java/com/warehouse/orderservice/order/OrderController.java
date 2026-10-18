package com.warehouse.orderservice.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    public record LineRequest(@NotBlank String sku, @Min(1) @Max(100) int qty) {
    }

    public record CreateOrderRequest(@NotEmpty @Size(max = 20) List<@Valid LineRequest> lines) {
    }

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    public ResponseEntity<OrderView> create(@Valid @RequestBody CreateOrderRequest request) {
        OrderView order = orders.create(request.lines().stream()
                .map(l -> new OrderService.Line(l.sku(), l.qty()))
                .toList());
        return ResponseEntity.created(URI.create("/api/orders/" + order.id())).body(order);
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderView> get(@PathVariable UUID id) {
        return ResponseEntity.of(orders.find(id));
    }

    @GetMapping
    public List<OrderView> recent(@RequestParam(defaultValue = "50") int limit) {
        return orders.recent(Math.min(Math.max(limit, 1), 500));
    }

    @ExceptionHandler(OrderService.UnknownSkuException.class)
    ProblemDetail unknownSku(OrderService.UnknownSkuException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
