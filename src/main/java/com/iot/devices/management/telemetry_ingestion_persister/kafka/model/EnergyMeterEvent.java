package com.iot.devices.management.telemetry_ingestion_persister.kafka.model;

import com.iot.devices.ThermostatMode;
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
@Document(collection = EnergyMeterEvent.ENERGY_METERS_COLLECTION)
public class EnergyMeterEvent implements TelemetryEvent{

    public static final String ENERGY_METERS_COLLECTION = "energy_meters";
    @Id
    private UUID deviceId;

    private Float voltage;

    private Float current;

    private Float power;

    private Float energyConsumed;

    private DeviceStatus status;

    private String firmwareVersion;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private Instant lastUpdated;

    @Transient
    private Long offset;
}
