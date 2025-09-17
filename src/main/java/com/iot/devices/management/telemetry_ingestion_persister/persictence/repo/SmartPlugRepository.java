package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartPlugEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface SmartPlugRepository extends TelemetryRepository<SmartPlugEvent> {

    Optional<SmartPlugEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
