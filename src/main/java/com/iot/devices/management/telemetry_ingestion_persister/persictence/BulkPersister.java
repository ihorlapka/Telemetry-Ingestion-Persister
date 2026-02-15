package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import com.iot.devices.management.telemetry_ingestion_persister.kafka.DeadLetterProducer;
import com.iot.devices.management.telemetry_ingestion_persister.metrics.KpiMetricLogger;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.PersistentEvent;
import com.mongodb.*;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.BiFunction;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.springframework.data.mongodb.core.BulkOperations.BulkMode.UNORDERED;

@Slf4j
@Component
@RequiredArgsConstructor
public class BulkPersister {

    public static final String ID_FIELD = "_id";
    public static final String TIMESTAMP_FIELD = "lastUpdated";

    private final RetryProperties retryProperties;
    private final MongoTemplate mongoTemplate;
    private final DeadLetterProducer deadLetterProducer;
    private final KpiMetricLogger kpiMetricLogger;

    public <T extends SpecificRecord, E extends PersistentEvent> void persistWithRetries(List<ConsumerRecord<String, T>> deviceTypeRecords, Class<E> clazz,
                                                                                         Set<Long> offsets, String collectionName, BiFunction<T, Long, E> mapping) {
        int currentTry = 0;
        Exception lastException = null;
        while (currentTry < retryProperties.getMaxAttempts()) {
            try {
                if (currentTry > 0) {
                    sleep(retryProperties.getWaitDuration());
                    kpiMetricLogger.incRetriesCount();
                }
                final List<E> eventsToBeInserted = getEventsToBeInserted(deviceTypeRecords, clazz, offsets, mapping);
                if (eventsToBeInserted.isEmpty()) {
                    log.info("All events had already been stored, their offsets will be committed");
                    deviceTypeRecords.stream().map(ConsumerRecord::offset).forEach(offsets::add);
                    return;
                }
                final BulkOperations bulkOps = mongoTemplate.bulkOps(UNORDERED, clazz, collectionName);
                bulkOps.insert(eventsToBeInserted);
                final long startTime = currentTimeMillis();
                final BulkWriteResult result = bulkOps.execute();
                final long batchPersistenceTimeMs = currentTimeMillis() - startTime;
                logKpis(clazz, batchPersistenceTimeMs, result);
                final List<Long> succeedOffsets = eventsToBeInserted.stream().map(PersistentEvent::getOffset).toList();
                offsets.addAll(succeedOffsets);
                log.info("Inserted: {} {} items, target: {}, avgEventTime: {}, {} offsets will be committed",
                        result.getInsertedCount(), clazz.getSimpleName(), eventsToBeInserted.size(),
                        batchPersistenceTimeMs / Math.max(1, result.getInsertedCount()), succeedOffsets.size());
                return;
            } catch (TransientDataAccessException | MongoSocketException | MongoTimeoutException | MongoWriteException |
                     MongoBulkWriteException e) {
                if (e instanceof MongoBulkWriteException bulkWriteException) {
                    final int insertedCount = bulkWriteException.getWriteResult().getInsertedCount();
                    if (insertedCount > 0) {
                        kpiMetricLogger.incStoredEventsDuringError(clazz.getSimpleName(), insertedCount);
                        log.info("Persistence of events resulted with error: {}, but {} events were stored ids: {}",
                                e.getMessage(), insertedCount, getInsertedIds(bulkWriteException));
                    }
                }
                log.warn("Failed to persist on try {}/{}. Waiting {} ms before next retry",
                        currentTry + 1, retryProperties.getMaxAttempts(), retryProperties.getWaitDuration());
                lastException = e;
                currentTry++;
            } catch (IllegalArgumentException | NullPointerException | InvalidMongoDbApiUsageException |
                     MongoCommandException e) {
                log.error("A non-retriable error occurred while persisting events, sending them to dead letter topic", e);
                kpiMetricLogger.incNonRetriableSkippedErrorsCount(e.getClass().getSimpleName());
                deadLetterProducer.send(getEventsToBeInserted(deviceTypeRecords, clazz, offsets, mapping), offsets);
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

    public Optional<Long> getMaxConsecutiveOffset(Set<Long> persistedOffsets) {
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

    private <T extends SpecificRecord, E extends PersistentEvent> List<E> getEventsToBeInserted(List<ConsumerRecord<String, T>> deviceTypeRecords, Class<E> clazz,
                                                                                                Set<Long> offsets, BiFunction<T, Long, E> mapping) {
        final List<E> events = mapEvents(deviceTypeRecords, mapping);
        final List<E> existingEvents = findAlreadyPresentEventsInDb(clazz, events);
        return filterPresentInDbEvents(offsets, events, existingEvents);
    }

    private <T extends SpecificRecord, E extends PersistentEvent> List<E> mapEvents(List<ConsumerRecord<String, T>> deviceTypeRecords,
                                                                                    BiFunction<T, Long, E> mapping) {
        return deviceTypeRecords.stream()
                .map(record -> mapping.apply(record.value(), record.offset()))
                .toList();
    }

    private <E extends PersistentEvent> List<E> findAlreadyPresentEventsInDb(Class<E> clazz, List<E> events) {
        final List<Criteria> criteriaList = events.stream()
                .map(event -> Criteria.where(ID_FIELD).is(event.getDeviceId()).and(TIMESTAMP_FIELD).is(event.getTimestamp()))
                .toList();
        final Query query = new Query(new Criteria().orOperator(criteriaList));
        query.fields().include(ID_FIELD);
        query.fields().include(TIMESTAMP_FIELD);
        final long startTime = currentTimeMillis();
        final List<E> telemetryEventsInDb = mongoTemplate.find(query, clazz);
        kpiMetricLogger.recordsFindEventsQueryTime(clazz.getSimpleName(), currentTimeMillis() - startTime);
        if (!telemetryEventsInDb.isEmpty()) {
            kpiMetricLogger.incAlreadyStoredEvents(telemetryEventsInDb.size());
        }
        return telemetryEventsInDb;
    }

    private <E extends PersistentEvent> List<E> filterPresentInDbEvents(Set<Long> offsets, List<E> events, List<E> existingEvents) {
        final List<E> filteredEvents = new ArrayList<>();
        for (E event : events) {
            if (existingEvents.contains(event)) {
                log.info("Skipping already stored event: {}, offset={} will be commited", event, event.getOffset());
                ofNullable(event.getOffset()).ifPresent(offsets::add);
            } else {
                filteredEvents.add(event);
            }
        }
        return filteredEvents;
    }

    private List<String> getInsertedIds(MongoBulkWriteException bulkWriteException) {
        return bulkWriteException.getWriteResult().getInserts().stream()
                .map(BulkWriteInsert::getId)
                .map(BsonValue::asString)
                .map(BsonString::getValue)
                .toList();
    }

    private <E extends PersistentEvent> void logKpis(Class<E> clazz, long batchPersistenceTimeMs, BulkWriteResult result) {
        kpiMetricLogger.recordBatchInsertTime(clazz.getSimpleName(), batchPersistenceTimeMs);
        kpiMetricLogger.recordAvgEventPersistenceTime(batchPersistenceTimeMs / Math.max(1, result.getInsertedCount()));
        kpiMetricLogger.recordInsertedEventsNumber(clazz.getSimpleName(), result.getInsertedCount());
    }
}
