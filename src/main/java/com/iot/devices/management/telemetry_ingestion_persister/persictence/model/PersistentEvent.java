package com.iot.devices.management.telemetry_ingestion_persister.persictence.model;

import java.time.Instant;
import java.util.UUID;

public interface PersistentEvent {

    UUID getDeviceId();
    Instant getTimestamp();
    Long getOffset();
}
