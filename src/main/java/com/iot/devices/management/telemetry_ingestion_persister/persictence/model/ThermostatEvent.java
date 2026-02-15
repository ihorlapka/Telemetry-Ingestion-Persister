package com.iot.devices.management.telemetry_ingestion_persister.persictence.model;

import com.iot.devices.ThermostatMode;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceStatus;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.TimeSeries;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.Instant;
import java.util.UUID;

import static org.springframework.data.mongodb.core.timeseries.Granularity.MINUTES;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"deviceId", "lastUpdated"})
@Document(collection = ThermostatEvent.THERMOSTATS_COLLECTION)
@TimeSeries(
        timeField = "lastUpdated",
        metaField = "deviceId",
        granularity = MINUTES
)
public class ThermostatEvent implements PersistentEvent {

    public static final String THERMOSTATS_COLLECTION = "thermostats";
    @Id
    private UUID deviceId;
    private Float currentTemperature;
    private Float targetTemperature;
    private Float humidity;
    private ThermostatMode mode;
    private DeviceStatus status;
    private String firmwareVersion;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private Instant lastUpdated;
    @Transient
    private Long offset;

    public Instant getTimestamp() {
        return lastUpdated;
    }
}
