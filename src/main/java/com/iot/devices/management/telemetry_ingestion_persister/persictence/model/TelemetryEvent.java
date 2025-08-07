package com.iot.devices.management.telemetry_ingestion_persister.persictence.model;

import java.time.Instant;
import java.util.UUID;

public interface TelemetryEvent {

    UUID getDeviceId();
    Instant getLastUpdated();
    Long getOffset();
}
