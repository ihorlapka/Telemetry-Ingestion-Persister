package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartLightEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface SmartLightRepository extends TelemetryRepository<SmartLightEvent> {

    Optional<SmartLightEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
