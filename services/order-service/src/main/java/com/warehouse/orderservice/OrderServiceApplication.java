package com.warehouse.orderservice;

import java.util.TimeZone;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

    public static void main(String[] args) {
        // The JDBC driver sends the JVM time zone to Postgres on connect, and the postgres image
        // rejects legacy names such as Asia/Calcutta. Run in UTC everywhere; timestamps are timestamptz.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
