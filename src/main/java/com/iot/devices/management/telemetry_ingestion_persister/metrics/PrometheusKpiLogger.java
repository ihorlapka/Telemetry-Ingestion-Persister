package com.iot.devices.management.telemetry_ingestion_persister.metrics;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class PrometheusKpiLogger implements KpiMetricLogger {

    private final AtomicInteger recordsInOnePoll = new AtomicInteger(0);
    private final AtomicInteger insertedDoorSensorsPerBatch = new AtomicInteger(0);
    private final AtomicInteger insertedThermostatsPerBatch = new AtomicInteger(0);
    private final AtomicInteger insertedSmartLightsPerBatch = new AtomicInteger(0);
    private final AtomicInteger insertedEnergyMetersPerBatch = new AtomicInteger(0);
    private final AtomicInteger insertedSmartPlugsPerBatch = new AtomicInteger(0);
    private final AtomicInteger insertedTemperatureSensorsPerBatch = new AtomicInteger(0);
    private final AtomicInteger insertedSoilMoistureSensorsPerBatch = new AtomicInteger(0);
    private final AtomicLong avgEventPersistenceTime = new AtomicLong(0);
    private final ConcurrentMap<String, Counter> nonRetriableSkippedErrorsCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> eventsPersistingTimeSummaries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> findEventsTimeSummaries = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry;
    private final Counter retriesCounter;
    private final Counter alreadyStoredEventsCounter;
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

        this.insertedEventsDuringErrorCounter = Counter.builder("tip_inserted_events_during_error_count")
                .description("The number of events which were stored during one failed insert operation")
                .register(meterRegistry);

        this.fatalErrorsCounter = Counter.builder("tip_fatal_errors_count")
                .description("The number of fatal errors")
                .register(meterRegistry);

        Gauge.builder("tip_records_per_poll_gauge", recordsInOnePoll, AtomicInteger::get)
                .description("The number of records received in one poll")
                .register(meterRegistry);

        Gauge.builder("tip_avg_event_persisting_time_gauge", avgEventPersistenceTime, AtomicLong::get)
                .description("The average time of persisting one event")
                .register(meterRegistry);

        for (DeviceType deviceType : DeviceType.values()) {
                Gauge.builder("tip_inserted_events_per_batch_gauge", getInsertedEventsFromDeviceType(deviceType), AtomicInteger::get)
                        .description("The number of events inserted per batch")
                        .tag("deviceType", deviceType.name())
                        .register(meterRegistry);
        }
    }

    @Override
    public void recordBatchInsertTime(String deviceType, long timeMs) {
        eventsPersistingTimeSummaries.computeIfAbsent(deviceType, k ->
                        DistributionSummary.builder("tip_device_persisting_time")
                                .description("The time during which patch operation finished successfully")
                                .tag("deviceType", deviceType)
                                .publishPercentiles(0.5, 0.9, 0.99)
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
                                .publishPercentiles(0.5, 0.9, 0.99)
                                .register(meterRegistry))
                .record(timeMs);
    }

    @Override
    public void incAlreadyStoredEvents(int alreadyStoredEventsAmount) {
        alreadyStoredEventsCounter.increment();
    }

    @Override
    public void recordAvgEventPersistenceTime(long timeMs) {
        avgEventPersistenceTime.set(timeMs);
    }

    @Override
    public void incStoredEventsDuringError(String eventType, int storedAmount) {
        insertedEventsDuringErrorCounter.increment();
    }

    @Override
    public void incFatalErrorsCount(String errorName) {
        fatalErrorsCounter.increment();
    }

    @Override
    public void recordRecordsInOnePoll(int recordsNumber) {
        recordsInOnePoll.set(recordsNumber);
    }

    @Override
    public void recordInsertedEventsNumber(String eventType, int insertedCount) {
        final AtomicInteger insertedEvents = getInsertedEventsClasses(eventType);
        insertedEvents.set(insertedCount);
    }

    private AtomicInteger getInsertedEventsFromDeviceType(DeviceType deviceType) {
        return switch (deviceType) {
            case DOOR_SENSOR -> insertedDoorSensorsPerBatch;
            case THERMOSTAT -> insertedThermostatsPerBatch;
            case SMART_LIGHT -> insertedSmartLightsPerBatch;
            case ENERGY_METER -> insertedEnergyMetersPerBatch;
            case SMART_PLUG -> insertedSmartPlugsPerBatch;
            case TEMPERATURE_SENSOR -> insertedTemperatureSensorsPerBatch;
            case SOIL_MOISTURE_SENSOR -> insertedSoilMoistureSensorsPerBatch;
        };
    }

    private AtomicInteger getInsertedEventsClasses(String event) {
        return switch (event) {
            case "DoorSensorEvent" -> insertedDoorSensorsPerBatch;
            case "ThermostatEvent" -> insertedThermostatsPerBatch;
            case "SmartLightEvent" -> insertedSmartLightsPerBatch;
            case "EnergyMeterEvent" -> insertedEnergyMetersPerBatch;
            case "SmartPlugEvent" -> insertedSmartPlugsPerBatch;
            case "TemperatureSensorEvent" -> insertedTemperatureSensorsPerBatch;
            case "SoilMoistureSensorEvent" -> insertedSoilMoistureSensorsPerBatch;
            default -> new AtomicInteger(0);
        };
    }

}
