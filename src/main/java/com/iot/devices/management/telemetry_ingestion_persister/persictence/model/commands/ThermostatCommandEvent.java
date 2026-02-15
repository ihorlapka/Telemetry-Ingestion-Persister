package com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.PersistentEvent;
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
@EqualsAndHashCode(of = "commandId")
@Document(collection = ThermostatCommandEvent.THERMOSTAT_COMMANDS_COLLECTION)
@TimeSeries(
        timeField = "createdAt",
        metaField = "deviceId",
        granularity = MINUTES
)
public class ThermostatCommandEvent implements PersistentEvent {

    public static final String THERMOSTAT_COMMANDS_COLLECTION = "thermostat_commands";
    @Id
    private UUID commandId;
    private UUID deviceId;
    private Float targetTemperature;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private Instant createdAt;
    @Transient
    private Long offset;

    @Override
    public Instant getTimestamp() {
        return createdAt;
    }
}
