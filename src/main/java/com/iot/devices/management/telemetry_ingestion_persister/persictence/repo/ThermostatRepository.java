package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.ThermostatEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.UUID;

public interface ThermostatRepository extends MongoRepository<ThermostatEvent, UUID> {
}
