package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import com.iot.devices.*;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.DeadLetterProducer;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceType;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.*;
import com.mongodb.*;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.BiFunction;

import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.DoorSensorEvent.DOOR_SENSORS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.EnergyMeterEvent.ENERGY_METERS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartLightEvent.SMART_LIGHTS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartPlugEvent.SMART_PLUGS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SoilMoistureSensorEvent.SOIL_MOISTER_SENSORS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.TemperatureSensorEvent.TEMPERATURE_SENSORS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.ThermostatEvent.THERMOSTATS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.mapping.EventsMapper.*;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceType.getDeviceTypeByName;
import static java.lang.Thread.sleep;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static org.springframework.data.mongodb.core.BulkOperations.BulkMode.UNORDERED;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryPersister {

    public static final String ID_FIELD = "_id";
    public static final String LAST_UPDATED_FIELD = "lastUpdated";

    private final RetryProperties retryProperties;
    private final MongoTemplate mongoTemplate;
    private final DeadLetterProducer deadLetterProducer;


    public Optional<OffsetAndMetadata> persist(List<ConsumerRecord<String, SpecificRecord>> records) {
        final Map<DeviceType, List<ConsumerRecord<String, SpecificRecord>>> recordsByEventType = groupRecordsByDeviceType(records);
        final Set<Long> offsets = new TreeSet<>();
        for (Map.Entry<DeviceType, List<ConsumerRecord<String, SpecificRecord>>> entry : recordsByEventType.entrySet()) {
            final DeviceType deviceType = entry.getKey();
            final List<ConsumerRecord<String, SpecificRecord>> recordsPerType = entry.getValue();
            switch (deviceType) {
                case THERMOSTAT ->
                        persistWithRetries(recordsPerType, ThermostatEvent.class, offsets, THERMOSTATS_COLLECTION,
                                (t, offset) -> mapThermostat((Thermostat) t, offset));
                case DOOR_SENSOR ->
                        persistWithRetries(recordsPerType, DoorSensorEvent.class, offsets, DOOR_SENSORS_COLLECTION,
                                (ds, offset) -> mapDoorSensor((DoorSensor) ds, offset));
                case SMART_LIGHT ->
                        persistWithRetries(recordsPerType, SmartLightEvent.class, offsets, SMART_LIGHTS_COLLECTION,
                                (sl, offset) -> mapSmartLight((SmartLight) sl, offset));
                case ENERGY_METER ->
                        persistWithRetries(recordsPerType, EnergyMeterEvent.class, offsets, ENERGY_METERS_COLLECTION,
                                (em, offset) -> mapEnergyMeter((EnergyMeter) em, offset));
                case SMART_PLUG ->
                        persistWithRetries(recordsPerType, SmartPlugEvent.class, offsets, SMART_PLUGS_COLLECTION,
                                (sp, offset) -> mapSmartPlug((SmartPlug) sp, offset));
                case TEMPERATURE_SENSOR ->
                        persistWithRetries(recordsPerType, TemperatureSensorEvent.class, offsets, TEMPERATURE_SENSORS_COLLECTION,
                                (sp, offset) -> mapTemperatureSensor((TemperatureSensor) sp, offset));
                case SOIL_MOISTURE_SENSOR ->
                        persistWithRetries(recordsPerType, SoilMoistureSensorEvent.class, offsets, SOIL_MOISTER_SENSORS_COLLECTION,
                                (sms, offset) -> mapSoilMoistureSensor((SoilMoistureSensor) sms, offset));
                default -> throw new IllegalArgumentException("Unknown device type detected");
            }
        }
        return getMaxConsecutiveOffset(offsets).map(OffsetAndMetadata::new);
    }

    private Map<DeviceType, List<ConsumerRecord<String, SpecificRecord>>> groupRecordsByDeviceType(List<ConsumerRecord<String, SpecificRecord>> records) {
        return records.stream().collect(groupingBy(record -> getDeviceTypeByName(record.value().getSchema().getName())));
    }

    private <T extends SpecificRecord> void persistWithRetries(List<ConsumerRecord<String, T>> deviceTypeRecords, Class<? extends TelemetryEvent> clazz,
                                                               Set<Long> offsets, String collectionName, BiFunction<T, Long, TelemetryEvent> mapping) {
        final List<TelemetryEvent> filteredEvents = new ArrayList<>();
        int currentTry = 0;
        Exception lastException = null;
        while (currentTry < retryProperties.getMaxAttempts()) {
            try {
                if (currentTry > 0) {
                    sleep(retryProperties.getWaitDuration());
                }
                final List<TelemetryEvent> events = mapEvents(deviceTypeRecords, mapping);
                final List<? extends TelemetryEvent> existingEvents = findAlreadyPresentEventsInDb(clazz, events);
                filteredEvents.addAll(filterPresentEvents(offsets, events, existingEvents));
                if (filteredEvents.isEmpty()) {
                    log.info("All events are already stored, all offsets will be committed");
                    deviceTypeRecords.stream().map(ConsumerRecord::offset).forEach(offsets::add);
                    return;
                }
                final BulkOperations bulkOps = mongoTemplate.bulkOps(UNORDERED, clazz, collectionName);
                bulkOps.insert(filteredEvents);
                final BulkWriteResult result = bulkOps.execute();
                log.info("Inserted: {} events, target: {}, all offsets will be committed", result.getInsertedCount(), filteredEvents.size());
                filteredEvents.forEach(event -> offsets.add(event.getOffset()));
                return;
            } catch (TransientDataAccessException | MongoSocketException | MongoTimeoutException | MongoWriteException | MongoBulkWriteException e) {
                final List<TelemetryEvent> storedEvents = new ArrayList<>(filteredEvents);
                if (e instanceof MongoBulkWriteException bulkWriteException) {
                    for (BulkWriteError error : bulkWriteException.getWriteErrors()) {
                        TelemetryEvent failedEvent = filteredEvents.get(error.getIndex());
                        storedEvents.remove(failedEvent);
                        log.warn("Failed to upsert event: {}, error: {}", failedEvent, error.getMessage());
                    }
                    if (!storedEvents.isEmpty()) {
                        storedEvents.forEach(event -> offsets.add(event.getOffset()));
                        log.info("Persistence of events resulted with error: {}, but some events were stored {}, their offsets may be committed",
                                e.getMessage(), storedEvents);
                    }
                }
                log.warn("Failed to persist on try {}/{}, offsets={}. Waiting {} ms before next retry, events {}",
                        currentTry + 1, retryProperties.getMaxAttempts(), getOffsets(deviceTypeRecords), retryProperties.getWaitDuration(), filteredEvents);
                filteredEvents.removeAll(storedEvents);
                lastException = e;
                currentTry++;
            } catch (IllegalArgumentException | NullPointerException | InvalidMongoDbApiUsageException | MongoCommandException e) {
                log.error("A non-recoverable error occurred while persisting events sending them to dead letter topic", e);
                if (!filteredEvents.isEmpty()) {
                    deadLetterProducer.send(filteredEvents);
                }
            } catch (Exception e) {
                log.error("A non-retriable error occurred while persisting record. Failing immediately.", e);
                throw new RuntimeException("Non-retriable error", e);
            }
        }
        log.error("All {} attempts to persist record failed.", retryProperties.getMaxAttempts(), lastException);
        throw new RuntimeException("Update failed after max retries.", lastException);
    }

    private <T extends SpecificRecord, E> List<E> mapEvents(List<ConsumerRecord<String, T>> deviceTypeRecords, BiFunction<T, Long, E> mapping) {
        return deviceTypeRecords.stream()
                .map(record -> mapping.apply(record.value(), record.offset()))
                .toList();
    }

    private List<? extends TelemetryEvent> findAlreadyPresentEventsInDb(Class<? extends TelemetryEvent> clazz, List<TelemetryEvent> events) {
        final List<Criteria> criteriaList = events.stream()
                .map(event -> Criteria.where(ID_FIELD).is(event.getDeviceId()).and(LAST_UPDATED_FIELD).is(event.getLastUpdated()))
                .toList();
        final Query query = new Query(new Criteria().orOperator(criteriaList));
        query.fields().include(ID_FIELD);
        query.fields().include(LAST_UPDATED_FIELD);
        return mongoTemplate.find(query, clazz);
    }

    private List<TelemetryEvent> filterPresentEvents(Set<Long> offsets, List<TelemetryEvent> events,
                                                     List<? extends TelemetryEvent> existingEvents) {
        final List<TelemetryEvent> filteredEvents = new ArrayList<>();
        for (TelemetryEvent event : events) {
            if (existingEvents.contains(event)) {
                log.info("Skipping already stored event: {}, offset={} will be commited", event, event.getOffset());
                ofNullable(event.getOffset()).ifPresent(offsets::add);
            } else {
                filteredEvents.add(event);
            }
        }
        return filteredEvents;
    }

    private <T extends SpecificRecord> List<Long> getOffsets(List<ConsumerRecord<String, T>> deviceTypeRecords) {
        return deviceTypeRecords.stream().map(ConsumerRecord::offset).toList();
    }

    private Optional<Long> getMaxConsecutiveOffset(Set<Long> persistedOffsets) {
        Optional<Long> max = persistedOffsets.stream().max(Comparator.comparingLong(o -> o));
        Optional<Long> min = persistedOffsets.stream().min(Comparator.comparingLong(o -> o));
        if (max.isPresent()) {
            long offsetToCommit = min.get();
            for (long offset : persistedOffsets) {
                if (offset == offsetToCommit) {
                    continue;
                }
                if (offset - offsetToCommit == 1) {
                    offsetToCommit = offset;
                } else {
                    log.info("Max consecutive offset is {}, min={}, max={}", offsetToCommit, min.get(), max.get());
                    break;
                }
            }
            return Optional.of(offsetToCommit + 1);
        }
        return empty();
    }
}
