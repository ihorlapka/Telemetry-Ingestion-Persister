package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import com.iot.devices.*;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.*;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.repo.*;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static com.iot.devices.management.telemetry_ingestion_persister.mapping.EventsMapper.*;
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

    @Override
    public Optional<OffsetAndMetadata> persist(List<ConsumerRecord<String, SpecificRecord>> records) {
        final ConcurrentSkipListSet<Long> offsets = new ConcurrentSkipListSet<>();
        final List<CompletableFuture<Void>> futures = new ArrayList<>(records.size());
        for (ConsumerRecord<String, SpecificRecord> record : records) {
            switch (record.value()) {
                case Thermostat t -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        () -> thermostatRepository.findByDeviceIdAndLastUpdated(UUID.fromString(t.getDeviceId()), t.getLastUpdated()),
                        () -> thermostatRepository.insert(mapThermostat(t, record.offset()))), executorService
                ));
                case DoorSensor ds -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        () -> doorSensorRepository.findByDeviceIdAndLastUpdated(UUID.fromString(ds.getDeviceId()), ds.getLastUpdated()),
                        () -> doorSensorRepository.insert(mapDoorSensor(ds, record.offset()))), executorService
                ));
                case SmartLight sl -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        () -> smartLightRepository.findByDeviceIdAndLastUpdated(UUID.fromString(sl.getDeviceId()), sl.getLastUpdated()),
                        () -> smartLightRepository.insert(mapSmartLight(sl, record.offset()))), executorService
                ));
                case EnergyMeter em -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        () -> energyMeterRepository.findByDeviceIdAndLastUpdated(UUID.fromString(em.getDeviceId()), em.getLastUpdated()),
                        () -> energyMeterRepository.insert(mapEnergyMeter(em, record.offset()))), executorService
                ));
                case SmartPlug sp -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        () -> smartPlugRepository.findByDeviceIdAndLastUpdated(UUID.fromString(sp.getDeviceId()), sp.getLastUpdated()),
                        () -> smartPlugRepository.insert(mapSmartPlug(sp, record.offset()))), executorService
                ));
                case TemperatureSensor ts -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        () -> temperatureSensorRepository.findByDeviceIdAndLastUpdated(UUID.fromString(ts.getDeviceId()), ts.getLastUpdated()),
                        () -> temperatureSensorRepository.insert(mapTemperatureSensor(ts, record.offset()))), executorService
                ));
                case SoilMoistureSensor sms -> futures.add(CompletableFuture.runAsync(() -> persistIfNotPresent(offsets, record,
                        () -> soilMoistureSensorRepository.findByDeviceIdAndLastUpdated(UUID.fromString(sms.getDeviceId()), sms.getLastUpdated()),
                        () -> soilMoistureSensorRepository.insert(mapSoilMoistureSensor(sms, record.offset()))), executorService
                ));
                default -> throw new IllegalArgumentException("Unknown device type detected");
            }
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return offsets.stream()
                .map(OffsetAndMetadata::new)
                .max(comparingLong(OffsetAndMetadata::offset));
    }

    private <T extends TelemetryEvent> void persistIfNotPresent(Set<Long> offsets, ConsumerRecord<String, SpecificRecord> record,
                                                                Supplier<Optional<T>> findTelemetryInDb,
                                                                Supplier<T> insert) {
        try {
            final Optional<T> telemetryInDb = findTelemetryInDb.get();
            if (telemetryInDb.isEmpty()) {
                final T inserted = insert.get();
                log.info("Inserted event: {}, offset:{}", inserted, record.offset());
            } else {
                log.info("Event is already stored {}, offset: {}", telemetryInDb, record.offset());
            }
            offsets.add(record.offset());
        } catch (Exception e) {
            log.error("Something bad happened!", e);//TODO: implement!
        }
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
