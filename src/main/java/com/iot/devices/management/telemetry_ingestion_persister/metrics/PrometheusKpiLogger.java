package com.iot.devices.management.telemetry_ingestion_persister.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class PrometheusKpiLogger implements KpiMetricLogger {

    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final ConcurrentMap<String, Counter> notUpdatedDevicesCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> severalUpdatedDevicesCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> nonRetriableErrorsCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> deviceUpdatingTimeSummaries = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry;
    private final Counter retriesCounter;

    public PrometheusKpiLogger(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        this.retriesCounter = Counter.builder("not_updated_devices_count")
                .description("The number of retries during patching device")
                .register(meterRegistry);

        Gauge.builder("parallel_persister_active_threads", activeThreads, AtomicInteger::get)
                .description("The number of threads currently executing tasks")
                .register(meterRegistry);
    }

    @Override
    public void recordInsertTime(String deviceType, long timeMs) {
        deviceUpdatingTimeSummaries.computeIfAbsent(deviceType, k ->
                        DistributionSummary.builder("device_updating_time")
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
    public void incNonRetriableErrorsCount(String errorName) {
        nonRetriableErrorsCounters.computeIfAbsent(errorName, (error) ->
                        Counter.builder("non_retriable_errors_count")
                                .description("The number of non-retriable exceptions")
                                .tag("error", error)
                                .register(meterRegistry))
                .increment();
    }

    @Override
    public void recordsFindEventsQueryTime(long time) {

    }

    @Override
    public void incAlreadyStoredEvents(int alreadyStoredEventsAmount) {

    }

    @Override
    public void incInsertedEventsInOneOperation(String eventType, int insertedCount) {

    }

    @Override
    public void incStoredEventsDuringError(String eventType, int storedAmount) {

    }

    @Override
    public void incFatalErrorsCount(String errorName) {

    }

}
