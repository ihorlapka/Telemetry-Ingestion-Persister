package com.iot.devices.management.telemetry_ingestion_persister.persictence.commands;

import com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceType;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.iot.devices.management.telemetry_ingestion_persister.persictence.enums.DeviceType.getDeviceTypeByCommandName;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

@Component
@Profile("mongoTemplate")
@RequiredArgsConstructor
public class ParallelCommandsPersister implements CommandsPersister {

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    private final CommandPersister commandPersister;


    public Optional<OffsetAndMetadata> persist(List<ConsumerRecord<String, SpecificRecord>> records) {
        final Map<DeviceType, List<ConsumerRecord<String, SpecificRecord>>> recordsByEventType = groupRecordsByDeviceType(records);
        final ConcurrentSkipListSet<OffsetAndMetadata> offsets = new ConcurrentSkipListSet<>(comparingLong(OffsetAndMetadata::offset));
        final List<CompletableFuture<Void>> futures = new ArrayList<>(recordsByEventType.size());
        for (Map.Entry<DeviceType, List<ConsumerRecord<String, SpecificRecord>>> entry : recordsByEventType.entrySet()) {
            final DeviceType deviceType = entry.getKey();
            final List<ConsumerRecord<String, SpecificRecord>> recordsPerType = entry.getValue();
            futures.add(CompletableFuture.runAsync(
                    () -> commandPersister.persist(deviceType, recordsPerType).ifPresent(offsets::add),
                    executorService)
            );
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return offsets.stream()
                .max(comparingLong(OffsetAndMetadata::offset));
    }

    private Map<DeviceType, List<ConsumerRecord<String, SpecificRecord>>> groupRecordsByDeviceType(List<ConsumerRecord<String, SpecificRecord>> records) {
        return records.stream().collect(groupingBy(
                record -> getDeviceTypeByCommandName(record.value().getSchema().getName()),
                LinkedHashMap::new,
                toList()));
    }
}
