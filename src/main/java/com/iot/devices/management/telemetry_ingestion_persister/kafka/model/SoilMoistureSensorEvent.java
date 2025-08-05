package com.iot.devices.management.telemetry_ingestion_persister.kafka.model;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceStatus;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"deviceId", "lastUpdated"})
@Document(collection = SoilMoistureSensorEvent.SOIL_MOISTER_SENSORS_COLLECTION)
public class SoilMoistureSensorEvent implements TelemetryEvent {

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
}
