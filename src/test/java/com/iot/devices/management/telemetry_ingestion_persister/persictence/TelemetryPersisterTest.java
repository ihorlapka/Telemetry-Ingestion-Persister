package com.iot.devices.management.telemetry_ingestion_persister.persictence;

import com.iot.devices.*;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.DeadLetterProducer;
import com.iot.devices.management.telemetry_ingestion_persister.metrics.KpiMetricLogger;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.DoorSensorEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartPlugEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.ThermostatEvent;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.WriteConcernError;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.iot.devices.DeviceStatus.*;
import static com.iot.devices.DoorState.CLOSED;
import static com.iot.devices.DoorState.OPEN;
import static com.iot.devices.ThermostatMode.*;
import static com.iot.devices.management.telemetry_ingestion_persister.mapping.EventsMapper.mapSmartPlug;
import static com.iot.devices.management.telemetry_ingestion_persister.mapping.EventsMapper.mapThermostat;
import static com.mongodb.bulk.BulkWriteResult.acknowledged;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.BulkOperations.BulkMode.UNORDERED;

@ExtendWith(MockitoExtension.class)
class TelemetryPersisterTest {

    static final String TOPIC = "topic";

    @Mock
    RetryProperties retryProperties;
    @Mock
    MongoTemplate mongoTemplate;
    @Mock
    DeadLetterProducer deadLetterProducer;
    @Mock
    BulkOperations bulkOperationsForDoorSensors;
    @Mock
    BulkOperations bulkOperationsForThermostats;
    @Mock
    BulkOperations bulkOperationsForSmartPlug;
    @Mock
    KpiMetricLogger kpiMetricLogger;

    @InjectMocks
    TelemetryPersister telemetryPersister;

    ParallelTelemetryPersister parallelTelemetryPersister;

    String deviceId1 = UUID.randomUUID().toString();
    String deviceId2 = UUID.randomUUID().toString();
    String deviceId3 = UUID.randomUUID().toString();
    String deviceId4 = UUID.randomUUID().toString();
    String deviceId5 = UUID.randomUUID().toString();
    String deviceId6 = UUID.randomUUID().toString();
    String deviceId7 = UUID.randomUUID().toString();
    String deviceId8 = UUID.randomUUID().toString();
    String deviceId9 = UUID.randomUUID().toString();
    String deviceId10 = UUID.randomUUID().toString();
    String deviceId11 = UUID.randomUUID().toString();
    String deviceId12 = UUID.randomUUID().toString();

    Instant nowTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    DoorSensor doorSensor1 = new DoorSensor(deviceId1, OPEN, 85, false, ONLINE, nowTime, "1.0.2v", nowTime);
    DoorSensor doorSensor2 = new DoorSensor(deviceId2, CLOSED, 11, false, OFFLINE, nowTime, "1.0.2v", nowTime);
    DoorSensor doorSensor3 = new DoorSensor(deviceId3, CLOSED, 100, true, ONLINE, nowTime, "1.0.2v", nowTime);
    DoorSensor doorSensor4 = new DoorSensor(deviceId4, OPEN, 45, false, OFFLINE, nowTime, "1.0.2v", nowTime);

    Thermostat thermostat5 = new Thermostat(deviceId5, 26.6f, 22.5f, 10.0f, COOL, ONLINE, "2.123v", nowTime);
    Thermostat thermostat6 = new Thermostat(deviceId6, 24f, 24.0f, 12.0f, AUTO, ONLINE, "2.123v", nowTime);
    Thermostat thermostat7 = new Thermostat(deviceId7, 18.6f, 22f, 4.0f, HEAT, ONLINE, "2.123v", nowTime);

    SmartPlug smartPlug8 = new SmartPlug(deviceId8, false, 230f, 227f, 99f, MAINTENANCE, null, nowTime.minus(5, ChronoUnit.MINUTES));
    SmartPlug smartPlug9 = new SmartPlug(deviceId9, true, 230f, 231f, 99f, ONLINE, "2.123v", nowTime.minus(4, ChronoUnit.MINUTES));
    SmartPlug smartPlug10 = new SmartPlug(deviceId10, false, 230f, 224f, 99f, OFFLINE, null, nowTime.minus(3, ChronoUnit.MINUTES));
    SmartPlug smartPlug11 = new SmartPlug(deviceId11, true, 230f, 229f, 99f, ONLINE, "1.123v", nowTime.minus(2, ChronoUnit.MINUTES));
    SmartPlug smartPlug12 = new SmartPlug(deviceId12, true, 230f, 227f, 99f, ONLINE, "1.123v", nowTime.minus(1, ChronoUnit.MINUTES));

    ConsumerRecord<String, SpecificRecord> record1 = new ConsumerRecord<>(TOPIC, 0, 0, doorSensor1.getDeviceId(), doorSensor1);
    ConsumerRecord<String, SpecificRecord> record2 = new ConsumerRecord<>(TOPIC, 0, 1, doorSensor2.getDeviceId(), doorSensor2);
    ConsumerRecord<String, SpecificRecord> record3 = new ConsumerRecord<>(TOPIC, 0, 2, doorSensor3.getDeviceId(), doorSensor3);
    ConsumerRecord<String, SpecificRecord> record4 = new ConsumerRecord<>(TOPIC, 0, 3, doorSensor4.getDeviceId(), doorSensor4);
    ConsumerRecord<String, SpecificRecord> record5 = new ConsumerRecord<>(TOPIC, 0, 4, thermostat5.getDeviceId(), thermostat5);
    ConsumerRecord<String, SpecificRecord> record6 = new ConsumerRecord<>(TOPIC, 0, 5, thermostat6.getDeviceId(), thermostat6);
    ConsumerRecord<String, SpecificRecord> record7 = new ConsumerRecord<>(TOPIC, 0, 6, thermostat7.getDeviceId(), thermostat7);
    ConsumerRecord<String, SpecificRecord> record8 = new ConsumerRecord<>(TOPIC, 0, 7, smartPlug8.getDeviceId(), smartPlug8);
    ConsumerRecord<String, SpecificRecord> record9 = new ConsumerRecord<>(TOPIC, 0, 8, smartPlug9.getDeviceId(), smartPlug9);
    ConsumerRecord<String, SpecificRecord> record10 = new ConsumerRecord<>(TOPIC, 0, 9, smartPlug10.getDeviceId(), smartPlug10);
    ConsumerRecord<String, SpecificRecord> record11 = new ConsumerRecord<>(TOPIC, 0, 10, smartPlug11.getDeviceId(), smartPlug11);
    ConsumerRecord<String, SpecificRecord> record12 = new ConsumerRecord<>(TOPIC, 0, 11, smartPlug12.getDeviceId(), smartPlug12);

    List<ConsumerRecord<String, SpecificRecord>> records = new ArrayList<>();

    @BeforeEach
    void setUp() {
        lenient().when(retryProperties.getMaxAttempts()).thenReturn(5);
        lenient().when(retryProperties.getWaitDuration()).thenReturn(500);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        records.add(record5);
        records.add(record6);
        records.add(record7);
        records.add(record8);
        records.add(record9);
        records.add(record10);
        records.add(record11);
        records.add(record12);

        parallelTelemetryPersister = new ParallelTelemetryPersister(telemetryPersister);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(retryProperties, mongoTemplate, deadLetterProducer, bulkOperationsForDoorSensors,
                bulkOperationsForThermostats, bulkOperationsForSmartPlug, kpiMetricLogger);
    }

    @Test
    void allSucceeded() {
        when(mongoTemplate.find(any(Query.class), any())).thenReturn(List.of());
        when(bulkOperationsForDoorSensors.execute()).thenReturn(acknowledged(INSERT, 4, 0, List.of(), List.of()));
        when(bulkOperationsForThermostats.execute()).thenReturn(acknowledged(INSERT, 3, 0, List.of(), List.of()));
        when(bulkOperationsForSmartPlug.execute()).thenReturn(acknowledged(INSERT, 5, 0, List.of(), List.of()));
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(DoorSensorEvent.class), eq(DoorSensorEvent.DOOR_SENSORS_COLLECTION))).thenReturn(bulkOperationsForDoorSensors);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(ThermostatEvent.class), eq(ThermostatEvent.THERMOSTATS_COLLECTION))).thenReturn(bulkOperationsForThermostats);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(SmartPlugEvent.class), eq(SmartPlugEvent.SMART_PLUGS_COLLECTION))).thenReturn(bulkOperationsForSmartPlug);

        Optional<OffsetAndMetadata> offsetAndMetadata = parallelTelemetryPersister.persist(records);

        assertTrue(offsetAndMetadata.isPresent());
        assertEquals(12, offsetAndMetadata.get().offset());
        verify(retryProperties, times(3)).getMaxAttempts();
        verify(mongoTemplate, times(3)).find(any(), any());
        verify(bulkOperationsForDoorSensors).insert(anyList());
        verify(bulkOperationsForThermostats).insert(anyList());
        verify(bulkOperationsForSmartPlug).insert(anyList());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(SmartPlugEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(SmartPlugEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger, times(3)).recordAvgEventPersistenceTime(anyLong());
        verify(kpiMetricLogger).recordInsertedEventsNumber(DoorSensorEvent.class.getSimpleName(), 4);
        verify(kpiMetricLogger).recordInsertedEventsNumber(ThermostatEvent.class.getSimpleName(), 3);
        verify(kpiMetricLogger).recordInsertedEventsNumber(SmartPlugEvent.class.getSimpleName(), 5);
    }

    @Test
    void storingAfterRetries() {
        lenient().when(mongoTemplate.find(any(Query.class), eq(ThermostatEvent.class))).thenReturn(List.of())
                        .thenReturn(List.of(mapThermostat(thermostat5, 4), mapThermostat(thermostat6, 5)))
                        .thenReturn(List.of(mapThermostat(thermostat5, 4), mapThermostat(thermostat6, 5)))
                        .thenReturn(List.of(mapThermostat(thermostat5, 4), mapThermostat(thermostat6, 5)));

        when(bulkOperationsForDoorSensors.execute()).thenReturn(acknowledged(INSERT, 4, 0, List.of(), List.of()));

        BulkWriteError bulkWriteError1 = new BulkWriteError(111, "some msg", new BsonDocument(), 2);
        BulkWriteError bulkWriteError2 = new BulkWriteError(111, "some msg", new BsonDocument(), 0);
        BulkWriteError bulkWriteError3 = new BulkWriteError(111, "some msg", new BsonDocument(), 0);

        MongoBulkWriteException mongoBulkWriteException1 = new MongoBulkWriteException(
                acknowledged(INSERT, 2, 0, List.of(), List.of(new BulkWriteInsert(0, new BsonString(deviceId5)), new BulkWriteInsert(0, new BsonString(deviceId6)))),
                List.of(bulkWriteError1), mock(WriteConcernError.class), new ServerAddress(), Set.of());
        MongoBulkWriteException mongoBulkWriteException2 = new MongoBulkWriteException(acknowledged(INSERT, 0, 0, List.of(), List.of()), List.of(bulkWriteError2), mock(WriteConcernError.class), new ServerAddress(), Set.of());
        MongoBulkWriteException mongoBulkWriteException3 = new MongoBulkWriteException(acknowledged(INSERT, 0, 0, List.of(), List.of()), List.of(bulkWriteError3), mock(WriteConcernError.class), new ServerAddress(), Set.of());

        when(bulkOperationsForThermostats.execute()).thenThrow(mongoBulkWriteException1, mongoBulkWriteException2, mongoBulkWriteException3)
                .thenReturn(acknowledged(INSERT, 1, 0, List.of(), List.of()));

        when(bulkOperationsForSmartPlug.execute()).thenReturn(acknowledged(INSERT, 5, 0, List.of(), List.of()));
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(DoorSensorEvent.class), eq(DoorSensorEvent.DOOR_SENSORS_COLLECTION))).thenReturn(bulkOperationsForDoorSensors);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(ThermostatEvent.class), eq(ThermostatEvent.THERMOSTATS_COLLECTION))).thenReturn(bulkOperationsForThermostats);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(SmartPlugEvent.class), eq(SmartPlugEvent.SMART_PLUGS_COLLECTION))).thenReturn(bulkOperationsForSmartPlug);

        Optional<OffsetAndMetadata> offsetAndMetadata = parallelTelemetryPersister.persist(records);

        assertTrue(offsetAndMetadata.isPresent());
        assertEquals(12, offsetAndMetadata.get().offset());
        verify(retryProperties, times(9)).getMaxAttempts();
        verify(retryProperties, times(6)).getWaitDuration();
        verify(mongoTemplate, times(6)).find(any(), any());
        verify(bulkOperationsForDoorSensors).insert(anyList());
        verify(bulkOperationsForThermostats, times(4)).insert(anyList());
        verify(bulkOperationsForSmartPlug).insert(anyList());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger, times(4)).recordsFindEventsQueryTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(SmartPlugEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(SmartPlugEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger, times(3)).recordAvgEventPersistenceTime(anyLong());
        verify(kpiMetricLogger).incStoredEventsDuringError(ThermostatEvent.class.getSimpleName(), 2);
        verify(kpiMetricLogger, times(3)).incAlreadyStoredEvents(2);
        verify(kpiMetricLogger, times(3)).incRetriesCount();
        verify(kpiMetricLogger).recordInsertedEventsNumber(DoorSensorEvent.class.getSimpleName(), 4);
        verify(kpiMetricLogger).recordInsertedEventsNumber(ThermostatEvent.class.getSimpleName(), 1);
        verify(kpiMetricLogger).recordInsertedEventsNumber(SmartPlugEvent.class.getSimpleName(), 5);
    }

    @Test
    void nonRetriableError() {
        when(bulkOperationsForDoorSensors.execute()).thenReturn(acknowledged(INSERT, 4, 0, List.of(), List.of()));
        when(bulkOperationsForThermostats.execute()).thenReturn(acknowledged(INSERT, 3, 0, List.of(), List.of()));
        when(bulkOperationsForSmartPlug.execute()).thenThrow(NullPointerException.class);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(DoorSensorEvent.class), eq(DoorSensorEvent.DOOR_SENSORS_COLLECTION))).thenReturn(bulkOperationsForDoorSensors);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(ThermostatEvent.class), eq(ThermostatEvent.THERMOSTATS_COLLECTION))).thenReturn(bulkOperationsForThermostats);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(SmartPlugEvent.class), eq(SmartPlugEvent.SMART_PLUGS_COLLECTION))).thenReturn(bulkOperationsForSmartPlug);

        Optional<OffsetAndMetadata> offsetAndMetadata = parallelTelemetryPersister.persist(records);

        assertTrue(offsetAndMetadata.isPresent());
        assertEquals(7, offsetAndMetadata.get().offset());
        verify(retryProperties, times(3)).getMaxAttempts();
        verify(mongoTemplate, times(4)).find(any(), any());
        verify(bulkOperationsForDoorSensors).insert(anyList());
        verify(bulkOperationsForThermostats).insert(anyList());
        verify(bulkOperationsForSmartPlug).insert(anyList());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger, times(2)).recordsFindEventsQueryTime(eq(SmartPlugEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger, times(2)).recordAvgEventPersistenceTime(anyLong());
        verify(kpiMetricLogger).incNonRetriableSkippedErrorsCount(NullPointerException.class.getSimpleName());
        verify(deadLetterProducer).send(eq(List.of(mapSmartPlug(smartPlug8, 7), mapSmartPlug(smartPlug9, 8), mapSmartPlug(smartPlug10, 9),
                mapSmartPlug(smartPlug11, 10), mapSmartPlug(smartPlug12, 11))), anySet());
        verify(kpiMetricLogger).recordInsertedEventsNumber(DoorSensorEvent.class.getSimpleName(), 4);
        verify(kpiMetricLogger).recordInsertedEventsNumber(ThermostatEvent.class.getSimpleName(), 3);
    }

    @Test
    void fatalError() {
        when(bulkOperationsForDoorSensors.execute()).thenReturn(acknowledged(INSERT, 4, 0, List.of(), List.of()));
        when(bulkOperationsForThermostats.execute()).thenReturn(acknowledged(INSERT, 3, 0, List.of(), List.of()));
        when(bulkOperationsForSmartPlug.execute()).thenThrow(UncategorizedMongoDbException.class);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(DoorSensorEvent.class), eq(DoorSensorEvent.DOOR_SENSORS_COLLECTION))).thenReturn(bulkOperationsForDoorSensors);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(ThermostatEvent.class), eq(ThermostatEvent.THERMOSTATS_COLLECTION))).thenReturn(bulkOperationsForThermostats);
        when(mongoTemplate.bulkOps(eq(UNORDERED), eq(SmartPlugEvent.class), eq(SmartPlugEvent.SMART_PLUGS_COLLECTION))).thenReturn(bulkOperationsForSmartPlug);

        Assertions.assertThrows(RuntimeException.class, () -> parallelTelemetryPersister.persist(records));

        verify(retryProperties, times(3)).getMaxAttempts();
        verify(mongoTemplate, times(3)).find(any(), any());
        verify(bulkOperationsForDoorSensors).insert(anyList());
        verify(bulkOperationsForThermostats).insert(anyList());
        verify(bulkOperationsForSmartPlug).insert(anyList());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordsFindEventsQueryTime(eq(SmartPlugEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(DoorSensorEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger).recordBatchInsertTime(eq(ThermostatEvent.class.getSimpleName()), anyLong());
        verify(kpiMetricLogger, times(2)).recordAvgEventPersistenceTime(anyLong());
        verify(kpiMetricLogger).incFatalErrorsCount(UncategorizedMongoDbException.class.getSimpleName());
        verify(kpiMetricLogger).recordInsertedEventsNumber(DoorSensorEvent.class.getSimpleName(), 4);
        verify(kpiMetricLogger).recordInsertedEventsNumber(ThermostatEvent.class.getSimpleName(), 3);
    }
}