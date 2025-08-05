package com.iot.devices.management.telemetry_ingestion_persister.mapping;

import com.iot.devices.*;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.model.*;
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

    public static SmartLightEvent mapSmartLight(SmartLight sl, long offset) {
        return new SmartLightEvent(
                UUID.fromString(sl.getDeviceId()),
                sl.getIsOn(),
                sl.getBrightness(),
                sl.getColor(),
                ofNullable(sl.getMode())
                        .map(Enum::name)
                        .orElse(null),
                sl.getPowerConsumption(),
                ofNullable(sl.getStatus())
                        .map(s -> DeviceStatus.valueOf(s.name()))
                        .orElse(null),
                sl.getFirmwareVersion(),
                sl.getLastUpdated(),
                offset);
    }

    public static EnergyMeterEvent mapEnergyMeter(EnergyMeter em, long offset) {
        return new EnergyMeterEvent(
                UUID.fromString(em.getDeviceId()),
                em.getVoltage(),
                em.getCurrent(),
                em.getPower(),
                em.getEnergyConsumed(),
                ofNullable(em.getStatus())
                        .map(s -> DeviceStatus.valueOf(s.name()))
                        .orElse(null),
                em.getFirmwareVersion(),
                em.getLastUpdated(),
                offset);
    }

    public static SmartPlugEvent mapSmartPlug(SmartPlug em, long offset) {
        return new SmartPlugEvent(
                UUID.fromString(em.getDeviceId()),
                em.getIsOn(),
                em.getVoltage(),
                em.getCurrent(),
                em.getPowerUsage(),
                ofNullable(em.getStatus())
                        .map(s -> DeviceStatus.valueOf(s.name()))
                        .orElse(null),
                em.getFirmwareVersion(),
                em.getLastUpdated(),
                offset);
    }

    public static TemperatureSensorEvent mapTemperatureSensor(TemperatureSensor ts, long offset) {
        return new TemperatureSensorEvent(
                UUID.fromString(ts.getDeviceId()),
                ts.getTemperature(),
                ts.getHumidity(),
                ts.getPressure(),
                ofNullable(ts.getUnit())
                        .map(Enum::name)
                        .orElse(null),
                ofNullable(ts.getStatus())
                        .map(s -> DeviceStatus.valueOf(s.name()))
                        .orElse(null),
                ts.getFirmwareVersion(),
                ts.getLastUpdated(),
                offset);
    }

    public static SoilMoistureSensorEvent mapSoilMoistureSensor(SoilMoistureSensor sms, long offset) {
        return new SoilMoistureSensorEvent(
                UUID.fromString(sms.getDeviceId()),
                sms.getMoisturePercentage(),
                sms.getSoilTemperature(),
                sms.getBatteryLevel(),
                ofNullable(sms.getStatus())
                        .map(s -> DeviceStatus.valueOf(s.name()))
                        .orElse(null),
                sms.getFirmwareVersion(),
                sms.getLastUpdated(),
                offset);
    }
}
