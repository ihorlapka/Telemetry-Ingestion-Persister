package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.ThermostatEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface ThermostatRepository extends TelemetryRepository<ThermostatEvent> {

    Optional<ThermostatEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
