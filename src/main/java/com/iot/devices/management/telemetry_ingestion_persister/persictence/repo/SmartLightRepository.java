package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartLightEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface SmartLightRepository extends MongoRepository<SmartLightEvent, UUID> {

    Optional<SmartLightEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
