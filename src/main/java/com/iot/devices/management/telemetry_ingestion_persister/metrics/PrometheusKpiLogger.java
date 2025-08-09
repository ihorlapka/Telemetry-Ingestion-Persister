package com.iot.devices.management.telemetry_ingestion_persister.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class PrometheusKpiLogger implements KpiMetricLogger {

    private final ConcurrentMap<String, Counter> nonRetriableSkippedErrorsCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> eventsPersistingTimeSummaries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> findEventsTimeSummaries = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry;
    private final Counter retriesCounter;
    private final Counter alreadyStoredEventsCounter;
    private final Counter insertedEventsCounter;
    private final Counter insertedEventsDuringErrorCounter;
    private final Counter fatalErrorsCounter;

    public PrometheusKpiLogger(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        this.retriesCounter = Counter.builder("tip_persistence_retries_count")
                .description("The number of retries during events storage")
                .register(meterRegistry);

        this.alreadyStoredEventsCounter = Counter.builder("tip_already_stored_events_count")
                .description("The number of events which were already stored during previous app run")
                .register(meterRegistry);

        this.insertedEventsCounter = Counter.builder("tip_inserted_events_count")
                .description("The number of events which were stored during one insert operation")
                .register(meterRegistry);

        this.insertedEventsDuringErrorCounter = Counter.builder("tip_inserted_events_during_error_count")
                .description("The number of events which were stored during one failed insert operation")
                .register(meterRegistry);

        this.fatalErrorsCounter = Counter.builder("tip_fatal_errors_count")
                .description("The number of fatal errors")
                .register(meterRegistry);
    }

    @Override
    public void recordInsertTime(String deviceType, long timeMs) {
        eventsPersistingTimeSummaries.computeIfAbsent(deviceType, k ->
                        DistributionSummary.builder("tip_device_persisting_time")
                                .description("The time during which patch operation finished successfully")
                                .tag("deviceType", deviceType)
                                .register(meterRegistry))
                .record(timeMs);
    }

    @Override
    public void incRetriesCount() {
        retriesCounter.increment();
    }

    @Override
    public void incNonRetriableSkippedErrorsCount(String errorName) {
        nonRetriableSkippedErrorsCounters.computeIfAbsent(errorName, (error) ->
                        Counter.builder("tip_non_retriable_errors_count")
                                .description("The number of non-retriable (skipped) exceptions")
                                .tag("error", error)
                                .register(meterRegistry))
                .increment();
    }

    @Override
    public void recordsFindEventsQueryTime(String eventType, long timeMs) {
        findEventsTimeSummaries.computeIfAbsent(eventType, k ->
                        DistributionSummary.builder("tip_find_events_time")
                                .description("The executions time of find events operation")
                                .tag("deviceType", eventType)
                                .register(meterRegistry))
                .record(timeMs);
    }

    @Override
    public void incAlreadyStoredEvents(int alreadyStoredEventsAmount) {
        alreadyStoredEventsCounter.increment();
    }

    @Override
    public void incInsertedEventsInOneOperation(String eventType, int insertedCount) {
        insertedEventsCounter.increment();
    }

    @Override
    public void incStoredEventsDuringError(String eventType, int storedAmount) {
        insertedEventsDuringErrorCounter.increment();
    }

    @Override
    public void incFatalErrorsCount(String errorName) {
        fatalErrorsCounter.increment();
    }

}
