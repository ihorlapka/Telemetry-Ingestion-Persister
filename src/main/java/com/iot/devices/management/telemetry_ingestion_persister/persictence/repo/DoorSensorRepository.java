package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.DoorSensorEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;


public interface DoorSensorRepository extends MongoRepository<DoorSensorEvent, UUID> {

    Optional<DoorSensorEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
