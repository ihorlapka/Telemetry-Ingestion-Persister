package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class Persister {


    public Map<TopicPartition, OffsetAndMetadata> persist(ConsumerRecords<String, SpecificRecord> records) {
        return null;
    }
}
