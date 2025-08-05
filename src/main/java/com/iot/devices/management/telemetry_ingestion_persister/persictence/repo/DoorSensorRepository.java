package com.iot.devices.management.telemetry_ingestion_persister.persictence.repo;

import com.iot.devices.management.telemetry_ingestion_persister.kafka.model.DoorSensorEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.UUID;


public interface DoorSensorRepository extends MongoRepository<DoorSensorEvent, UUID> {
}
