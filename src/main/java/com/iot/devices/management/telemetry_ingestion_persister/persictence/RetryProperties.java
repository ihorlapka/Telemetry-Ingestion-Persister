package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Slf4j
@Getter
@Setter
@ToString
@Configuration
@ConfigurationProperties(RetryProperties.PROPERTIES_PREFIX)
@RequiredArgsConstructor
public class RetryProperties {

    final static String PROPERTIES_PREFIX = "persister.retries";

    @Value("${" + PROPERTIES_PREFIX + ".max.attempts}")
    private int maxAttempts;

    @Value("${" + PROPERTIES_PREFIX + ".wait.duration.ms}")
    private int waitDuration;

    @PostConstruct
    private void logProperties() {
        log.info("Retry properties: {}", this);
    }
}
