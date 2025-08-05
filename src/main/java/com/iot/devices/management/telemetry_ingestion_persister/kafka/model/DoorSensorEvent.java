package com.iot.devices.management.telemetry_ingestion_persister.kafka.model;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceStatus;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DoorState;
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
@Document(collection = DoorSensorEvent.DOOR_SENSORS_COLLECTION)
public class DoorSensorEvent implements TelemetryEvent {

    public static final String DOOR_SENSORS_COLLECTION = "door_sensors";
    @Id
    private UUID deviceId;

    private DoorState doorState;

    private Integer batteryLevel;

    private Boolean tamperAlert;

    private DeviceStatus status;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private Instant lastOpened;

    private String firmwareVersion;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private Instant lastUpdated;

    @Transient
    private Long offset;
}
