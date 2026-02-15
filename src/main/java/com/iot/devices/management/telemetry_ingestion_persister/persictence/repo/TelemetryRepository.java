package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.PersistentEvent;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.NoRepositoryBean;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@NoRepositoryBean
public interface TelemetryRepository<T extends PersistentEvent> extends MongoRepository<T, UUID> {

    Optional<T> findByDeviceIdAndLastUpdated(UUID deviceId, Instant lastUpdated);
}
