package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.TemperatureSensorEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface TemperatureSensorRepository extends TelemetryRepository<TemperatureSensorEvent> {

    Optional<TemperatureSensorEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
