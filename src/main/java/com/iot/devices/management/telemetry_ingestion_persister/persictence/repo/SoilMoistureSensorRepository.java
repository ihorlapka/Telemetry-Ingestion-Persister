package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SoilMoistureSensorEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface SoilMoistureSensorRepository extends MongoRepository<SoilMoistureSensorEvent, UUID> {

    Optional<SoilMoistureSensorEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
