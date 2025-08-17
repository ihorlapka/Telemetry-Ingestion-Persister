package com.iot.devices.management.telemetry_ingestion_persister.metrics;

public interface KpiMetricLogger {
    void recordBatchInsertTime(String deviceType, long timeMs);
    void incRetriesCount();
    void incNonRetriableSkippedErrorsCount(String errorName);
    void recordsFindEventsQueryTime(String eventType, long timeMs);
    void incAlreadyStoredEvents(int alreadyStoredEventsAmount);
    void recordAvgEventPersistenceTime(long timeMs);
    void incStoredEventsDuringError(String eventType, int storedAmount);
    void incFatalErrorsCount(String errorName);
    void recordRecordsInOnePoll(int recordsNumber);
    void recordInsertedEventsNumber(String eventType, int insertedCount);
}
