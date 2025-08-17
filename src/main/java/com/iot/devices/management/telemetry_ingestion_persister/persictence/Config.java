package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static java.util.concurrent.TimeUnit.SECONDS;

@Configuration
public class Config {

    private static final String PROPERTIES_PREFIX = "mongo.pool";

    @Bean
    public MongoClientSettingsBuilderCustomizer customizer(@Value("${" + PROPERTIES_PREFIX + ".minSize}") int minSize,
                                                           @Value("${" + PROPERTIES_PREFIX + ".maxSize}") int maxSize,
                                                           @Value("${" + PROPERTIES_PREFIX + ".maxConnectionIdleTime}") int maxConnectionIdleTime,
                                                           @Value("${" + PROPERTIES_PREFIX + ".maxConnectionLifeTime}") int maxConnectionLifeTime) {
        return builder -> builder.applyToConnectionPoolSettings(pool ->
                pool.minSize(minSize)
                        .maxSize(maxSize)
                        .maxConnectionIdleTime(maxConnectionIdleTime, SECONDS)
                        .maxConnectionLifeTime(maxConnectionLifeTime, SECONDS)
        );
    }
}
