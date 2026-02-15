package com.iot.devices.management.telemetry_ingestion_persister.persictence.telemetries;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.List;
import java.util.Optional;

public interface TelemetriesPersister {

    Optional<OffsetAndMetadata> persist(List<ConsumerRecord<String, SpecificRecord>> records);
}
