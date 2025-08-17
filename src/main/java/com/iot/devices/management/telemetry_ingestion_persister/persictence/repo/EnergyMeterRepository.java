package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.EnergyMeterEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface EnergyMeterRepository extends MongoRepository<EnergyMeterEvent, UUID> {

    Optional<EnergyMeterEvent> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
