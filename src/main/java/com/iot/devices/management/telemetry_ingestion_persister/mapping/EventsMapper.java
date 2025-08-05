package com.iot.devices.management.telemetry_ingestion_persister.mapping;

import com.iot.devices.DoorSensor;
import com.iot.devices.Thermostat;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.model.DoorSensorEvent;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.model.ThermostatEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceStatus;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DoorState;
import lombok.experimental.UtilityClass;

import java.util.UUID;

import static java.util.Optional.ofNullable;

@UtilityClass
public class EventsMapper {

    public static DoorSensorEvent mapDoorSensor(DoorSensor ds, long offset) {
        return new DoorSensorEvent(
                UUID.fromString(ds.getDeviceId()),
                ofNullable(ds.getDoorState())
                        .map(s -> DoorState.valueOf(s.name()))
                        .orElse(null),
                ds.getBatteryLevel(),
                ds.getTamperAlert(),
                ofNullable(ds.getStatus())
                        .map(s -> DeviceStatus.valueOf(s.name()))
                        .orElse(null),
                ds.getLastOpened(),
                ds.getFirmwareVersion(),
                ds.getLastUpdated(),
                offset);
    }

    public static ThermostatEvent mapThermostat(Thermostat t, long offset) {
        return new ThermostatEvent(
                UUID.fromString(t.getDeviceId()),
                t.getCurrentTemperature(),
                t.getTargetTemperature(),
                t.getHumidity(),
                t.getMode(),
                ofNullable(t.getStatus())
                        .map(s -> DeviceStatus.valueOf(s.name()))
                        .orElse(null),
                t.getFirmwareVersion(),
                t.getLastUpdated(),
                offset);
    }
}
