package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import com.iot.devices.DoorSensor;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceStatus;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DoorState;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.repo.DoorSensorRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Optional.ofNullable;

@Slf4j
@Component
@RequiredArgsConstructor
public class Persister {

    private final DoorSensorRepository doorSensorRepository;


    public Map<TopicPartition, OffsetAndMetadata> persist(ConsumerRecords<String, SpecificRecord> records) {
        if (records.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new ConcurrentHashMap<>();
        for (ConsumerRecord<String, SpecificRecord> record: records) {
            switch (record.value()) {
                case DoorSensor ds -> doorSensorRepository.save(mapDoorSensor(ds));
                default -> throw new IllegalArgumentException("Unknown device type detected");
            }
            offsetsToCommit.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
        }
        return offsetsToCommit;
    }

    private DoorSensorEvent mapDoorSensor(DoorSensor ds) {
        return new DoorSensorEvent(
                UUID.fromString(ds.getDeviceId()),
                ofNullable(ds.getDoorState()).map(s -> DoorState.valueOf(s.name())).orElse(null),
                ds.getBatteryLevel(),
                ds.getTamperAlert(),
                ofNullable(ds.getStatus()).map(s -> DeviceStatus.valueOf(s.name())).orElse(null),
                ds.getLastOpened(),
                ds.getFirmwareVersion(),
                ds.getLastUpdated());
    }
}
