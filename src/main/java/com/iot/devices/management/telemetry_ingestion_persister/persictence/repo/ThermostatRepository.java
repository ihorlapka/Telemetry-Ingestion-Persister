package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.ThermostatEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface ThermostatRepository extends MongoRepository<ThermostatEvent, UUID> {

    Optional<ThermostatEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
