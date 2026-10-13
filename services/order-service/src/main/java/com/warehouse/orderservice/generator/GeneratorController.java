package com.warehouse.orderservice.generator;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/generator")
public class GeneratorController {

    public record Rate(@Min(0) @Max(600) int perMinute) {
    }

    private final OrderGenerator generator;

    public GeneratorController(OrderGenerator generator) {
        this.generator = generator;
    }

    @GetMapping
    public Rate get() {
        return new Rate(generator.perMinute());
    }

    @PutMapping
    public Rate set(@Valid @RequestBody Rate rate) {
        generator.setPerMinute(rate.perMinute());
        return get();
    }
}
