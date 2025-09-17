package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.EnergyMeterEvent;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface EnergyMeterRepository extends TelemetryRepository<EnergyMeterEvent> {

    Optional<EnergyMeterEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
