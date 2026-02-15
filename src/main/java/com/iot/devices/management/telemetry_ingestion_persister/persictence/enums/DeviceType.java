package com.iot.devices.management.telemetry_ingestion_persister.persictence.enums;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum DeviceType {
    THERMOSTAT("Thermostat", "ThermostatCommand"),
    DOOR_SENSOR("DoorSensor", "DoorSensorCommand"),
    SMART_LIGHT("SmartLight", "SmartLightCommand"),
    ENERGY_METER("EnergyMeter", "EnergyMeterCommand"),
    SMART_PLUG("SmartPlug", "SmartPlugCommand"),
    TEMPERATURE_SENSOR("TemperatureSensor", "TemperatureSensorCommand"),
    SOIL_MOISTURE_SENSOR("SoilMoistureSensor", "SoilMoistureSensorCommand");

    private final String telemetryName;
    private final String commandName;

    public static DeviceType getDeviceTypeByTelemetryName(String name) {
        for (DeviceType deviceType : values()) {
            if (name.equals(deviceType.telemetryName)) {
                return deviceType;
            }
        }
        throw new IllegalArgumentException("No DeviceType is present with telemetry name: " + name);
    }

    public static DeviceType getDeviceTypeByCommandName(String name) {
        for (DeviceType deviceType : values()) {
            if (name.equals(deviceType.commandName)) {
                return deviceType;
            }
        }
        throw new IllegalArgumentException("No DeviceType is present with command name: " + name);
    }
}
