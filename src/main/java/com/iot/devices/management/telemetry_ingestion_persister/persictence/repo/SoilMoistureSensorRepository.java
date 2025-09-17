package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SoilMoistureSensorEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface SoilMoistureSensorRepository extends TelemetryRepository<SoilMoistureSensorEvent> {

    Optional<SoilMoistureSensorEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
