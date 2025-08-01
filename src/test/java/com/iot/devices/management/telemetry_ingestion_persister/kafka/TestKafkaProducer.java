package com.iot.devices.management.telemetry_ingestion_persister.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class TestKafkaProducer {

    private final KafkaProducerProperties producerProperties;
    private final KafkaProducer<String, SpecificRecord> kafkaProducer;


    public TestKafkaProducer(KafkaProducerProperties producerProperties) {
        Properties properties = new Properties();
        properties.putAll(producerProperties.getProperties());
        this.kafkaProducer = new KafkaProducer<>(properties);
        this.producerProperties = producerProperties;
    }

    public void sendMessage(SpecificRecord record, String key) {
        try {
            final ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<>(producerProperties.getTopic(), key, record);
            final Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
            final RecordMetadata recordMetadata = future.get();
            if (recordMetadata.hasOffset()) {
                log.info("Message is successfully sent {}", record);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to send message: {}", record, e);
            throw new RuntimeException("Unable to send message!");
        }

    }
}
