package com.iot.devices.management.telemetry_ingestion_persister.persictence.model;

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
@Document(collection = SoilMoistureSensorEvent.SOIL_MOISTER_SENSORS_COLLECTION)
@TimeSeries(
        timeField = "lastUpdated",
        metaField = "deviceId",
        granularity = MINUTES
)
public class SoilMoistureSensorEvent implements PersistentEvent {

    public static final String SOIL_MOISTER_SENSORS_COLLECTION = "soil_moisture_sensors";
    @Id
    private UUID deviceId;
    private Float moisturePercentage;
    private Float soilTemperature;
    private Integer batteryLevel;
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
