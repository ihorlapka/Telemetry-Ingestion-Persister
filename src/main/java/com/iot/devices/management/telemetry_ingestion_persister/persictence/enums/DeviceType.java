package com.iot.devices.management.telemetry_ingestion_persister.persictence.enums;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum DeviceType {
    THERMOSTAT("Thermostat"),
    DOOR_SENSOR("DoorSensor"),
    SMART_LIGHT("SmartLight"),
    ENERGY_METER("EnergyMeter"),
    SMART_PLUG("SmartPlug"),
    TEMPERATURE_SENSOR("TemperatureSensor"),
    SOIL_MOISTURE_SENSOR("SoilMoistureSensor");

    private final String clearName;

    public static DeviceType getDeviceTypeByName(String name) {
        for (DeviceType deviceType : values()) {
            if (name.equals(deviceType.clearName)) {
                return deviceType;
            }
        }
        throw new IllegalArgumentException("No DeviceType is present with name: " + name);
    }
}
