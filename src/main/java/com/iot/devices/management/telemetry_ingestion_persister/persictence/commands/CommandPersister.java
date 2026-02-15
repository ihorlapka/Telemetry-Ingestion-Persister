package com.iot.devices.management.telemetry_ingestion_persister.persictence.commands;

import com.iot.commands.SmartLightCommand;
import com.iot.commands.SmartPlugCommand;
import com.iot.commands.ThermostatCommand;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.BulkPersister;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceType;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands.SmartLightCommandEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands.SmartPlugCommandEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands.ThermostatCommandEvent;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.iot.devices.management.telemetry_ingestion_persister.mapping.CommandsMapper.*;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartLightEvent.SMART_LIGHTS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartPlugEvent.SMART_PLUGS_COLLECTION;
import static com.iot.devices.management.telemetry_ingestion_persister.persictence.model.commands.ThermostatCommandEvent.THERMOSTAT_COMMANDS_COLLECTION;

@Component
@RequiredArgsConstructor
public class CommandPersister {

    private final BulkPersister bulkPersister;

    public Optional<OffsetAndMetadata> persist(DeviceType deviceType, List<ConsumerRecord<String, SpecificRecord>> recordsPerType) {
        final ConcurrentSkipListSet<Long> offsets = new ConcurrentSkipListSet<>();
        switch (deviceType) {
            case THERMOSTAT -> bulkPersister.persistWithRetries(recordsPerType, ThermostatCommandEvent.class, offsets, THERMOSTAT_COMMANDS_COLLECTION,
                    (t, offset) -> mapThermostat((ThermostatCommand) t, offset));
            case SMART_LIGHT -> bulkPersister.persistWithRetries(recordsPerType, SmartLightCommandEvent.class, offsets, SMART_LIGHTS_COLLECTION,
                    (sl, offset) -> mapSmartLight((SmartLightCommand) sl, offset));
            case SMART_PLUG -> bulkPersister.persistWithRetries(recordsPerType, SmartPlugCommandEvent.class, offsets, SMART_PLUGS_COLLECTION,
                    (sp, offset) -> mapSmartPlug((SmartPlugCommand) sp, offset));
            default -> throw new IllegalArgumentException("Persisting commands for " + deviceType + " is not implemented yet!");
        }
        return bulkPersister.getMaxConsecutiveOffset(offsets).map(OffsetAndMetadata::new);
    }
}
