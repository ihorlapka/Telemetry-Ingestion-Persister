package com.iot.devices.management.telemetry_ingestion_persister.kafka;

import com.iot.devices.management.telemetry_ingestion_persister.kafka.properties.KafkaConsumerProperties;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.Persister;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerRunner {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Collection<TopicPartition> partitions = new ArrayList<>();
    private volatile boolean isShutdown = false;
    private volatile boolean isSubscribed = false;

    private final Persister persister;
    private final KafkaConsumerProperties consumerProperties;
    private final AtomicBoolean kafkaConsumerStatusMonitor;
    private final MeterRegistry meterRegistry;

    private KafkaConsumer<String, SpecificRecord> kafkaConsumer;
    private KafkaClientMetrics kafkaClientMetrics;


    @PostConstruct
    public void pollMessages() {
        executorService.submit(this::runConsumer);
    }

    private void runConsumer() {
        while (!isShutdown) {
            try {
                if (!isSubscribed) {
                    subscribe();
                }
                final ConsumerRecords<String, SpecificRecord> records = kafkaConsumer.poll(Duration.of(consumerProperties.getPollTimeoutMs(), MILLIS));
                final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = persister.persist(records);

                if (!offsetsToCommit.isEmpty()) {
                    kafkaConsumer.commitAsync(offsetsToCommit, getOffsetCommitCallback());
                }
            } catch (WakeupException e) {
                log.info("Consumer poll woken up");
            } catch (Exception e) {
                log.error("Unexpected exception in consumer loop ", e);
                closeConsumer();
            }
        }
        log.info("Exited kafka consumer loop");
        closeConsumer();
    }

    private void subscribe() {
        final Properties properties = new Properties(consumerProperties.getProperties().size());
        properties.putAll(consumerProperties.getProperties());
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaClientMetrics = new KafkaClientMetrics(kafkaConsumer);
        kafkaClientMetrics.bindTo(meterRegistry);

        kafkaConsumer.subscribe(List.of(consumerProperties.getTopic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                log.info("Partitions revoked");
                partitions.clear();
                isSubscribed = false;
                kafkaConsumerStatusMonitor.set(false);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                log.info("Partitions assigned: {}", collection);
                partitions.addAll(collection);
                isSubscribed = true;
                kafkaConsumerStatusMonitor.set(!partitions.isEmpty());
            }
        });
    }



    private OffsetCommitCallback getOffsetCommitCallback() {
        return (committedOffsets, ex) -> {
            if (ex == null) {
                log.info("Async commit successful for offsets: {}", committedOffsets);
            } else {
                log.error("Async commit failed for offsets: {}. Error: {}", committedOffsets, ex.getMessage());
                if (ex instanceof KafkaException) {
                    log.error("Kafka commit error: {}", ex.getMessage());
                }
            }
        };
    }

    private void closeConsumer() {
        try {
            if (kafkaConsumer != null) {
                log.info("Closing kafka consumer");
                kafkaConsumer.close();
                log.info("Kafka consumer is closed");
                if (kafkaClientMetrics != null) {
                    log.warn("Closing kafka consumer metrics");
                    kafkaClientMetrics.close();
                    log.info("Kafka consumer metrics are closed");
                }
                isSubscribed = false;
            }
            if (!isShutdown) {
                log.info("Waiting {} ms before consumer restart", consumerProperties.getRestartTimeoutMs());
                Thread.sleep(consumerProperties.getRestartTimeoutMs());
            }
        } catch (InterruptedException e) {
            log.error("Failed to wait for consumer restart because thread was interrupted", e);
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    private void shutdown() throws InterruptedException {
        isShutdown = true;
        executorService.shutdown();
        if (!executorService.awaitTermination(consumerProperties.getExecutorTerminationTimeoutMs(), MILLISECONDS)) {
            executorService.shutdownNow();
            log.info("Kafka consumer executor shutdown forced");
        } else {
            log.info("Kafka consumer executor shutdown gracefully");
        }
    }
}
