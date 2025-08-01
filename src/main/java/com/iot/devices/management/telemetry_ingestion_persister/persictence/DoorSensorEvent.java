package com.iot.devices.management.telemetry_ingestion_persister.persictence;


import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceStatus;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DoorState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "door_sensors")
public class DoorSensorEvent {

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
}
