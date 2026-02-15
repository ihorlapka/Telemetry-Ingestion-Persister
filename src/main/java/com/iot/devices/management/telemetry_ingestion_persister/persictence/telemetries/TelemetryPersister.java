package com.iot.devices.management.telemetry_ingestion_persister.persictence.telemetries;

import com.iot.devices.*;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.BulkPersister;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceType;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.DoorSensorEvent.DOOR_SENSORS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.EnergyMeterEvent.ENERGY_METERS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartLightEvent.SMART_LIGHTS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartPlugEvent.SMART_PLUGS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SoilMoistureSensorEvent.SOIL_MOISTER_SENSORS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.TemperatureSensorEvent.TEMPERATURE_SENSORS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.ThermostatEvent.THERMOSTATS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.mapping.EventsMapper.*;

@Slf4j
@Component
@Profile("mongoTemplate")
@RequiredArgsConstructor
public class TelemetryPersister {

    private final BulkPersister bulkPersister;


    public Optional<OffsetAndMetadata> persist(DeviceType deviceType, List<ConsumerRecord<String, SpecificRecord>> recordsPerType) {
        final ConcurrentSkipListSet<Long> offsets = new ConcurrentSkipListSet<>();
        switch (deviceType) {
            case THERMOSTAT ->
                    bulkPersister.persistWithRetries(recordsPerType, ThermostatEvent.class, offsets, THERMOSTATS_COLLECTION,
                            (t, offset) -> mapThermostat((Thermostat) t, offset));
            case DOOR_SENSOR ->
                    bulkPersister.persistWithRetries(recordsPerType, DoorSensorEvent.class, offsets, DOOR_SENSORS_COLLECTION,
                            (ds, offset) -> mapDoorSensor((DoorSensor) ds, offset));
            case SMART_LIGHT ->
                    bulkPersister.persistWithRetries(recordsPerType, SmartLightEvent.class, offsets, SMART_LIGHTS_COLLECTION,
                            (sl, offset) -> mapSmartLight((SmartLight) sl, offset));
            case ENERGY_METER ->
                    bulkPersister.persistWithRetries(recordsPerType, EnergyMeterEvent.class, offsets, ENERGY_METERS_COLLECTION,
                            (em, offset) -> mapEnergyMeter((EnergyMeter) em, offset));
            case SMART_PLUG ->
                    bulkPersister.persistWithRetries(recordsPerType, SmartPlugEvent.class, offsets, SMART_PLUGS_COLLECTION,
                    (sp, offset) -> mapSmartPlug((SmartPlug) sp, offset));
            case TEMPERATURE_SENSOR ->
                    bulkPersister.persistWithRetries(recordsPerType, TemperatureSensorEvent.class, offsets, TEMPERATURE_SENSORS_COLLECTION,
                            (sp, offset) -> mapTemperatureSensor((TemperatureSensor) sp, offset));
            case SOIL_MOISTURE_SENSOR ->
                    bulkPersister.persistWithRetries(recordsPerType, SoilMoistureSensorEvent.class, offsets, SOIL_MOISTER_SENSORS_COLLECTION,
                            (sms, offset) -> mapSoilMoistureSensor((SoilMoistureSensor) sms, offset));
            default -> throw new IllegalArgumentException("Unknown device type detected");
        }
        return bulkPersister.getMaxConsecutiveOffset(offsets).map(OffsetAndMetadata::new);
    }
}
