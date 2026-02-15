package com.iot.devices.management.telemetry_ingestion_persister.persictence.telemetries;

import com.iot.devices.*;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.DeadLetterProducer;
import com.iot.devices.management.telemetry_ingestion_persister.metrics.KpiMetricLogger;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.RetryProperties;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.*;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.repo.*;
import com.mongodb.*;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.iot.devices.management.telemetry_ingestion_persister.mapping.EventsMapper.*;
import static java.lang.Thread.sleep;
import static java.util.Comparator.comparingLong;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Component
@Profile("mongoRepositories")
@RequiredArgsConstructor
public class RepositoryBasedTelemetryPersister implements TelemetriesPersister {

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    private final DoorSensorRepository doorSensorRepository;
    private final EnergyMeterRepository energyMeterRepository;
    private final SmartLightRepository smartLightRepository;
    private final SmartPlugRepository smartPlugRepository;
    private final SoilMoistureSensorRepository soilMoistureSensorRepository;
    private final TemperatureSensorRepository temperatureSensorRepository;
    private final ThermostatRepository thermostatRepository;
    private final RetryProperties retryProperties;
    private final KpiMetricLogger kpiMetricLogger;
    private final DeadLetterProducer deadLetterProducer;

    @Override
    public Optional<OffsetAndMetadata> persist(List<ConsumerRecord<String, SpecificRecord>> records) {
        final ConcurrentSkipListSet<Long> offsets = new ConcurrentSkipListSet<>();
        final List<CompletableFuture<Void>> futures = new ArrayList<>(records.size());
        for (ConsumerRecord<String, SpecificRecord> record : records) {
            switch (record.value()) {
                case Thermostat t -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        thermostatRepository, mapThermostat(t, record.offset())), executorService)
                );
                case DoorSensor ds -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        doorSensorRepository, mapDoorSensor(ds, record.offset())), executorService)
                );
                case SmartLight sl -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        smartLightRepository, mapSmartLight(sl, record.offset())), executorService)
                );
                case EnergyMeter em -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        energyMeterRepository, mapEnergyMeter(em, record.offset())), executorService)
                );
                case SmartPlug sp -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        smartPlugRepository, mapSmartPlug(sp, record.offset())), executorService)
                );
                case TemperatureSensor ts -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        temperatureSensorRepository, mapTemperatureSensor(ts, record.offset())), executorService)
                );
                case SoilMoistureSensor sms -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        soilMoistureSensorRepository, mapSoilMoistureSensor(sms, record.offset())), executorService)
                );
                default -> throw new IllegalArgumentException("Unknown device type detected");
            }
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return offsets.stream()
                .map(OffsetAndMetadata::new)
                .max(comparingLong(OffsetAndMetadata::offset));
    }

    private <T extends PersistentEvent> void persistIfNotPresent(Set<Long> offsets, ConsumerRecord<String, SpecificRecord> record,
                                                                 TelemetryRepository<T> mongoRepository, T event) {
        int currentTry = 0;
        Exception lastException = null;
        while (currentTry < retryProperties.getMaxAttempts()) {
            try {
                if (currentTry > 0) {
                    sleep(retryProperties.getWaitDuration());
                    kpiMetricLogger.incRetriesCount();
                }
                final Optional<T> telemetryInDb = mongoRepository.findByDeviceIdAndLastUpdated(event.getDeviceId(), event.getTimestamp());
                if (telemetryInDb.isEmpty()) {
                    final T inserted = mongoRepository.insert(event);
                    log.info("Inserted event: {}, offset:{}", inserted, record.offset());
                } else {
                    log.info("Event is already stored {}, offset: {}", telemetryInDb, record.offset());
                }
                offsets.add(record.offset());
                return;
            } catch (TransientDataAccessException
                     | RecoverableDataAccessException
                     | MongoSocketException
                     | MongoNotPrimaryException
                     | MongoNodeIsRecoveringException
                     | MongoCursorNotFoundException
                     | MongoTimeoutException e) {
                log.warn("Failed to persist on try {}/{}, offset={}. Waiting {} ms before next retry, event: {}",
                        currentTry + 1, retryProperties.getMaxAttempts(), record.offset(), retryProperties.getWaitDuration(), record.value());
                lastException = e;
                currentTry++;
            } catch (IllegalArgumentException | NullPointerException | InvalidMongoDbApiUsageException |
                     MongoCommandException e) {
                log.error("A non-retriable error occurred while persisting events, sending them to dead letter topic", e);
                kpiMetricLogger.incNonRetriableSkippedErrorsCount(e.getClass().getSimpleName());
                deadLetterProducer.send(List.of(event), offsets);
                return;
            } catch (Exception e) {
                log.error("Fatal error occurred while persisting record. Failing immediately, no offsets will be committed", e);
                kpiMetricLogger.incFatalErrorsCount(e.getClass().getSimpleName());
                throw new RuntimeException("Fatal error", e);
            }
        }
        log.error("All {} attempts to persist record failed.", retryProperties.getMaxAttempts(), lastException);
        throw new RuntimeException("Update failed after max retries.", lastException);
    }

    @PreDestroy
    private void shutdown() throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(10, SECONDS)) {
            executorService.shutdownNow();
            log.info("Executor shutdown forced");
        } else {
            log.info("Executor shutdown gracefully");
        }
    }
}
