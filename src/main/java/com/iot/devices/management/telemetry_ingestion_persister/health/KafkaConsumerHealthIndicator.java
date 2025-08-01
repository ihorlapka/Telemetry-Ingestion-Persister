package com.iot.devices.management.telemetry_ingestion_persister.health;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final AtomicBoolean kafkaConsumerStatusMonitor;

    @Override
    public Health health() {
        return kafkaConsumerStatusMonitor.get()
                ? Health.up().build()
                : Health.down().withDetails(Map.of("kafkaConsumer", "partitions are not assigned")).build();
    }
}
