package com.iot.devices.management.telemetry_ingestion_persister.kafka;

import com.iot.devices.management.telemetry_ingestion_persister.kafka.properties.KafkaConsumerProperties;
import com.iot.devices.management.telemetry_ingestion_persister.metrics.KpiMetricLogger;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.TelemetriesPersister;
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
import java.util.function.Function;

import static java.lang.Thread.sleep;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerRunner {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Collection<TopicPartition> partitions = new ArrayList<>();
    private volatile boolean isShutdown = false;
    private volatile boolean isSubscribed = false;

    private final TelemetriesPersister telemetryPersister;
    private final KafkaConsumerProperties consumerProperties;
    private final AtomicBoolean kafkaConsumerStatusMonitor;
    private final MeterRegistry meterRegistry;
    private final KpiMetricLogger kpiMetricLogger;

    private KafkaConsumer<String, SpecificRecord> kafkaConsumer;
    private KafkaClientMetrics kafkaClientMetrics;


    @PostConstruct
    public void pollMessages() {
        executorService.submit(this::runConsumer);
    }

    private void runConsumer() {
        if (!isSubscribed) {
            log.info("Subscribing...");
            subscribe();
        }
        while (!isShutdown) {
            try {
                final ConsumerRecords<String, SpecificRecord> records = kafkaConsumer.poll(Duration.of(consumerProperties.getPollTimeoutMs(), MILLIS));
                kpiMetricLogger.recordRecordsInOnePoll(records.count());
                final Set<TopicPartition> recordsPartitions = records.partitions();
                final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecord>>> recordsPerPartition = recordsPartitions.stream()
                        .collect(toMap(Function.identity(), records::records));

                final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(recordsPerPartition.size());
                for (Map.Entry<TopicPartition, List<ConsumerRecord<String, SpecificRecord>>> entry : recordsPerPartition.entrySet()) {
                    final Optional<OffsetAndMetadata> offsetToCommit = telemetryPersister.persist(entry.getValue());
                    offsetToCommit.ifPresent(offset -> offsetsToCommit.put(entry.getKey(), offset));
                }
                if (!offsetsToCommit.isEmpty()) {
                    kafkaConsumer.commitAsync(offsetsToCommit, getOffsetCommitCallback());
                }
            } catch (WakeupException e) {
                log.info("Consumer poll is woken up");
                if (!isShutdown) {
                    log.error("Unexpected kafka consumer poll wakeup", e);
                    throw e;
                }
            } catch (Exception e) {
                log.error("Unexpected exception in consumer loop ", e);
                closeConsumer();
                if (!isShutdown) {
                    log.info("Subscribing after kafka consumer was closed...");
                    subscribe();
                }
            }
        }
        log.info("Exited kafka consumer polling loop");
    }

    private void subscribe() {
        try {
            final Properties properties = new Properties(consumerProperties.getProperties().size());
            properties.putAll(consumerProperties.getProperties());
            kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaClientMetrics = new KafkaClientMetrics(kafkaConsumer);
            kafkaClientMetrics.bindTo(meterRegistry);
            log.info("Kafka Consumer is created");

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
        } catch (Exception e) {
            log.error("Unexpected error occurred during subscribing", e);
            throw e;
        }
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
                if (isShutdown) {
                    log.info("Kafka consumer poll wakeup...");
                    kafkaConsumer.wakeup();
                }
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
                sleep(consumerProperties.getRestartTimeoutMs());
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
