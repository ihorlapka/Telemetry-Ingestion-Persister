package com.iot.devices.management.telemetry_ingestion_persister.mapping;

import com.iot.commands.*;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands.SmartLightCommandEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands.SmartPlugCommandEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands.ThermostatCommandEvent;
import lombok.experimental.UtilityClass;

import java.util.UUID;

import static java.util.Optional.ofNullable;

@UtilityClass
public class CommandsMapper {

    public static ThermostatCommandEvent mapThermostat(ThermostatCommand t, long offset) {
        return new ThermostatCommandEvent(
                UUID.fromString(t.getCommandId()),
                UUID.fromString(t.getDeviceId()),
                t.getTargetTemperature(),
                t.getCreatedAt(),
                offset);
    }

    public static SmartLightCommandEvent mapSmartLight(SmartLightCommand sl, long offset) {
        return new SmartLightCommandEvent(
                UUID.fromString(sl.getCommandId()),
                UUID.fromString(sl.getDeviceId()),
                sl.getIsOn(),
                sl.getBrightness(),
                sl.getColor(),
                ofNullable(sl.getMode())
                        .map(Enum::name)
                        .orElse(null),
                sl.getCreatedAt(),
                offset);
    }

    public static SmartPlugCommandEvent mapSmartPlug(SmartPlugCommand em, long offset) {
        return new SmartPlugCommandEvent(
                UUID.fromString(em.getCommandId()),
                UUID.fromString(em.getDeviceId()),
                em.getIsOn(),
                em.getCreatedAt(),
                offset);
    }
}
