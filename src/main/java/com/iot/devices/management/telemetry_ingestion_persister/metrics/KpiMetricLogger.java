package com.iot.devices.management.telemetry_ingestion_persister.metrics;

public interface KpiMetricLogger {
    void recordInsertTime(String deviceType, long l);
    void incRetriesCount();
    void incNonRetriableErrorsCount(String errorName);
    void recordsFindEventsQueryTime(long time);
    void incAlreadyStoredEvents(int alreadyStoredEventsAmount);
    void incInsertedEventsInOneOperation(String eventType, int insertedCount);
    void incStoredEventsDuringError(String eventType, int storedAmount);
    void incFatalErrorsCount(String errorName);
}
