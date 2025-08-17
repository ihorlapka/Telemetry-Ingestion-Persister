package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.TemperatureSensorEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface TemperatureSensorRepository extends MongoRepository<TemperatureSensorEvent, UUID> {

    Optional<TemperatureSensorEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
