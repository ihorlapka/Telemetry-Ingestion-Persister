package com.iot.devices.management.telemetry_ingestion_persister.persictence.telemetries;

import com.iot.devices.DoorSensor;
import com.iot.devices.SmartPlug;
import com.iot.devices.Thermostat;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.DeadLetterProducer;
import com.iot.devices.management.telemetry_ingestion_persister.metrics.KpiMetricLogger;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.RetryProperties;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.DoorSensorEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.SmartPlugEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.model.ThermostatEvent;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.repo.*;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.CannotAcquireLockException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.LongStream;

import static com.iot.devices.DeviceStatus.*;
import static com.iot.devices.DeviceStatus.OFFLINE;
import static com.iot.devices.DeviceStatus.ONLINE;
import static com.iot.devices.DoorState.CLOSED;
import static com.iot.devices.DoorState.OPEN;
import static com.iot.devices.ThermostatMode.*;
import static com.iot.devices.management.telemetry_ingestion_persister.mapping.EventsMapper.*;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RepositoryBasedTelemetryPersisterTest {

    static final String TOPIC = "topic";

    @Mock
    DoorSensorRepository doorSensorRepository;
    @Mock
    EnergyMeterRepository energyMeterRepository;
    @Mock
    SmartLightRepository smartLightRepository;
    @Mock
    SmartPlugRepository smartPlugRepository;
    @Mock
    SoilMoistureSensorRepository soilMoistureSensorRepository;
    @Mock
    TemperatureSensorRepository temperatureSensorRepository;
    @Mock
    ThermostatRepository thermostatRepository;
    @Mock
    RetryProperties retryProperties;
    @Mock
    KpiMetricLogger kpiMetricLogger;
    @Mock
    DeadLetterProducer deadLetterProducer;

    @InjectMocks
    RepositoryBasedTelemetryPersister persister;

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

    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(retryProperties, doorSensorRepository, energyMeterRepository, smartLightRepository, smartPlugRepository,
                soilMoistureSensorRepository, temperatureSensorRepository, thermostatRepository, deadLetterProducer, kpiMetricLogger);
    }

    @Test
    void allSucceeded() {
        when(doorSensorRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        DoorSensorEvent doorSensorEvent1 = mapDoorSensor(doorSensor1, 0);
        DoorSensorEvent doorSensorEvent2 = mapDoorSensor(doorSensor2, 1);
        DoorSensorEvent doorSensorEvent3 = mapDoorSensor(doorSensor3, 2);
        DoorSensorEvent doorSensorEvent4 = mapDoorSensor(doorSensor4, 3);
        when(doorSensorRepository.insert(doorSensorEvent1)).thenReturn(doorSensorEvent1);
        when(doorSensorRepository.insert(doorSensorEvent2)).thenReturn(doorSensorEvent2);
        when(doorSensorRepository.insert(doorSensorEvent3)).thenReturn(doorSensorEvent3);
        when(doorSensorRepository.insert(doorSensorEvent4)).thenReturn(doorSensorEvent4);

        when(thermostatRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        ThermostatEvent thermostatEvent1 = mapThermostat(thermostat5, 0);
        ThermostatEvent thermostatEvent2 = mapThermostat(thermostat6, 1);
        ThermostatEvent thermostatEvent3 = mapThermostat(thermostat7, 2);
        when(thermostatRepository.insert(thermostatEvent1)).thenReturn(thermostatEvent1);
        when(thermostatRepository.insert(thermostatEvent2)).thenReturn(thermostatEvent2);
        when(thermostatRepository.insert(thermostatEvent3)).thenReturn(thermostatEvent3);

        when(smartPlugRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        SmartPlugEvent smartPlugEvent1 = mapSmartPlug(smartPlug8, 0);
        SmartPlugEvent smartPlugEvent2 = mapSmartPlug(smartPlug9, 1);
        SmartPlugEvent smartPlugEvent3 = mapSmartPlug(smartPlug10, 2);
        SmartPlugEvent smartPlugEvent4 = mapSmartPlug(smartPlug11, 3);
        SmartPlugEvent smartPlugEvent5 = mapSmartPlug(smartPlug12, 3);
        when(smartPlugRepository.insert(smartPlugEvent1)).thenReturn(smartPlugEvent1);
        when(smartPlugRepository.insert(smartPlugEvent2)).thenReturn(smartPlugEvent2);
        when(smartPlugRepository.insert(smartPlugEvent3)).thenReturn(smartPlugEvent3);
        when(smartPlugRepository.insert(smartPlugEvent4)).thenReturn(smartPlugEvent4);
        when(smartPlugRepository.insert(smartPlugEvent5)).thenReturn(smartPlugEvent5);

        Optional<OffsetAndMetadata> offsetAndMetadata = persister.persist(records);
        assertTrue(offsetAndMetadata.isPresent());
        assertEquals(11, offsetAndMetadata.get().offset());

        verify(doorSensorRepository, times(4)).findByDeviceIdAndLastUpdated(any(), any());
        verify(thermostatRepository, times(3)).findByDeviceIdAndLastUpdated(any(), any());
        verify(smartPlugRepository, times(5)).findByDeviceIdAndLastUpdated(any(), any());
        verify(retryProperties, times(12)).getMaxAttempts();
    }

    @Test
    void storingAfterRetries() {
        when(doorSensorRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        DoorSensorEvent doorSensorEvent1 = mapDoorSensor(doorSensor1, 0);
        DoorSensorEvent doorSensorEvent2 = mapDoorSensor(doorSensor2, 1);
        DoorSensorEvent doorSensorEvent3 = mapDoorSensor(doorSensor3, 2);
        DoorSensorEvent doorSensorEvent4 = mapDoorSensor(doorSensor4, 3);
        when(doorSensorRepository.insert(doorSensorEvent1)).thenReturn(doorSensorEvent1);
        when(doorSensorRepository.insert(doorSensorEvent2)).thenReturn(doorSensorEvent2);
        when(doorSensorRepository.insert(doorSensorEvent3)).thenReturn(doorSensorEvent3);
        when(doorSensorRepository.insert(doorSensorEvent4)).thenReturn(doorSensorEvent4);

        when(thermostatRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        ThermostatEvent thermostatEvent1 = mapThermostat(thermostat5, 0);
        ThermostatEvent thermostatEvent2 = mapThermostat(thermostat6, 1);
        ThermostatEvent thermostatEvent3 = mapThermostat(thermostat7, 2);
        when(thermostatRepository.insert(thermostatEvent1)).thenReturn(thermostatEvent1);
        when(thermostatRepository.insert(thermostatEvent2)).thenReturn(thermostatEvent2);
        when(thermostatRepository.insert(thermostatEvent3))
                .thenThrow(new CannotAcquireLockException("Some exception 1"))
                .thenThrow(new CannotAcquireLockException("Some exception 2"))
                .thenThrow(new CannotAcquireLockException("Some exception 3"))
                .thenReturn(thermostatEvent3);

        when(smartPlugRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        SmartPlugEvent smartPlugEvent1 = mapSmartPlug(smartPlug8, 0);
        SmartPlugEvent smartPlugEvent2 = mapSmartPlug(smartPlug9, 1);
        SmartPlugEvent smartPlugEvent3 = mapSmartPlug(smartPlug10, 2);
        SmartPlugEvent smartPlugEvent4 = mapSmartPlug(smartPlug11, 3);
        SmartPlugEvent smartPlugEvent5 = mapSmartPlug(smartPlug12, 3);
        when(smartPlugRepository.insert(smartPlugEvent1)).thenReturn(smartPlugEvent1);
        when(smartPlugRepository.insert(smartPlugEvent2)).thenReturn(smartPlugEvent2);
        when(smartPlugRepository.insert(smartPlugEvent3)).thenReturn(smartPlugEvent3);
        when(smartPlugRepository.insert(smartPlugEvent4)).thenReturn(smartPlugEvent4);
        when(smartPlugRepository.insert(smartPlugEvent5)).thenReturn(smartPlugEvent5);

        Optional<OffsetAndMetadata> offsetAndMetadata = persister.persist(records);
        assertTrue(offsetAndMetadata.isPresent());
        assertEquals(11, offsetAndMetadata.get().offset());

        verify(doorSensorRepository, times(4)).findByDeviceIdAndLastUpdated(any(), any());
        verify(thermostatRepository, times(6)).findByDeviceIdAndLastUpdated(any(), any());
        verify(smartPlugRepository, times(5)).findByDeviceIdAndLastUpdated(any(), any());
        verify(retryProperties, times(18)).getMaxAttempts();
        verify(retryProperties, times(6)).getWaitDuration();
        verify(kpiMetricLogger, times(3)).incRetriesCount();
    }

    @Test
    void nonRetriableError() {
        when(doorSensorRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        DoorSensorEvent doorSensorEvent1 = mapDoorSensor(doorSensor1, 0);
        DoorSensorEvent doorSensorEvent2 = mapDoorSensor(doorSensor2, 1);
        DoorSensorEvent doorSensorEvent3 = mapDoorSensor(doorSensor3, 2);
        DoorSensorEvent doorSensorEvent4 = mapDoorSensor(doorSensor4, 3);
        when(doorSensorRepository.insert(doorSensorEvent1)).thenReturn(doorSensorEvent1);
        when(doorSensorRepository.insert(doorSensorEvent2)).thenReturn(doorSensorEvent2);
        when(doorSensorRepository.insert(doorSensorEvent3)).thenReturn(doorSensorEvent3);
        when(doorSensorRepository.insert(doorSensorEvent4)).thenReturn(doorSensorEvent4);

        when(thermostatRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        ThermostatEvent thermostatEvent1 = mapThermostat(thermostat5, 0);
        ThermostatEvent thermostatEvent2 = mapThermostat(thermostat6, 1);
        ThermostatEvent thermostatEvent3 = mapThermostat(thermostat7, 2);
        when(thermostatRepository.insert(thermostatEvent1)).thenReturn(thermostatEvent1);
        when(thermostatRepository.insert(thermostatEvent2)).thenReturn(thermostatEvent2);
        when(thermostatRepository.insert(thermostatEvent3))
                .thenThrow(new NullPointerException());

        when(smartPlugRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        SmartPlugEvent smartPlugEvent1 = mapSmartPlug(smartPlug8, 0);
        SmartPlugEvent smartPlugEvent2 = mapSmartPlug(smartPlug9, 1);
        SmartPlugEvent smartPlugEvent3 = mapSmartPlug(smartPlug10, 2);
        SmartPlugEvent smartPlugEvent4 = mapSmartPlug(smartPlug11, 3);
        SmartPlugEvent smartPlugEvent5 = mapSmartPlug(smartPlug12, 3);
        when(smartPlugRepository.insert(smartPlugEvent1)).thenReturn(smartPlugEvent1);
        when(smartPlugRepository.insert(smartPlugEvent2)).thenReturn(smartPlugEvent2);
        when(smartPlugRepository.insert(smartPlugEvent3)).thenReturn(smartPlugEvent3);
        when(smartPlugRepository.insert(smartPlugEvent4)).thenReturn(smartPlugEvent4);
        when(smartPlugRepository.insert(smartPlugEvent5)).thenReturn(smartPlugEvent5);

        Optional<OffsetAndMetadata> offsetAndMetadata = persister.persist(records);
        assertTrue(offsetAndMetadata.isPresent());
        assertEquals(11, offsetAndMetadata.get().offset());

        verify(doorSensorRepository, times(4)).findByDeviceIdAndLastUpdated(any(), any());
        verify(thermostatRepository, times(3)).findByDeviceIdAndLastUpdated(any(), any());
        verify(smartPlugRepository, times(5)).findByDeviceIdAndLastUpdated(any(), any());
        verify(retryProperties, times(12)).getMaxAttempts();
        verify(deadLetterProducer).send(List.of(thermostatEvent3), LongStream.rangeClosed(0, 11).boxed().collect(toSet()));
        verify(kpiMetricLogger).incNonRetriableSkippedErrorsCount(anyString());
    }

    @Test
    void fatalError() {
        when(doorSensorRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        DoorSensorEvent doorSensorEvent1 = mapDoorSensor(doorSensor1, 0);
        DoorSensorEvent doorSensorEvent2 = mapDoorSensor(doorSensor2, 1);
        DoorSensorEvent doorSensorEvent3 = mapDoorSensor(doorSensor3, 2);
        DoorSensorEvent doorSensorEvent4 = mapDoorSensor(doorSensor4, 3);
        when(doorSensorRepository.insert(doorSensorEvent1)).thenReturn(doorSensorEvent1);
        when(doorSensorRepository.insert(doorSensorEvent2)).thenReturn(doorSensorEvent2);
        when(doorSensorRepository.insert(doorSensorEvent3)).thenReturn(doorSensorEvent3);
        when(doorSensorRepository.insert(doorSensorEvent4)).thenReturn(doorSensorEvent4);

        when(thermostatRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        ThermostatEvent thermostatEvent1 = mapThermostat(thermostat5, 0);
        ThermostatEvent thermostatEvent2 = mapThermostat(thermostat6, 1);
        ThermostatEvent thermostatEvent3 = mapThermostat(thermostat7, 2);
        when(thermostatRepository.insert(thermostatEvent1)).thenReturn(thermostatEvent1);
        when(thermostatRepository.insert(thermostatEvent2)).thenReturn(thermostatEvent2);
        when(thermostatRepository.insert(thermostatEvent3))
                .thenThrow(new RuntimeException("Unexpected error"));

        when(smartPlugRepository.findByDeviceIdAndLastUpdated(any(), any())).thenReturn(Optional.empty());
        SmartPlugEvent smartPlugEvent1 = mapSmartPlug(smartPlug8, 0);
        SmartPlugEvent smartPlugEvent2 = mapSmartPlug(smartPlug9, 1);
        SmartPlugEvent smartPlugEvent3 = mapSmartPlug(smartPlug10, 2);
        SmartPlugEvent smartPlugEvent4 = mapSmartPlug(smartPlug11, 3);
        SmartPlugEvent smartPlugEvent5 = mapSmartPlug(smartPlug12, 3);
        when(smartPlugRepository.insert(smartPlugEvent1)).thenReturn(smartPlugEvent1);
        when(smartPlugRepository.insert(smartPlugEvent2)).thenReturn(smartPlugEvent2);
        when(smartPlugRepository.insert(smartPlugEvent3)).thenReturn(smartPlugEvent3);
        when(smartPlugRepository.insert(smartPlugEvent4)).thenReturn(smartPlugEvent4);
        when(smartPlugRepository.insert(smartPlugEvent5)).thenReturn(smartPlugEvent5);

        Optional<OffsetAndMetadata> offsetAndMetadata = persister.persist(records);
        assertTrue(offsetAndMetadata.isPresent());
        assertEquals(11, offsetAndMetadata.get().offset());

        verify(doorSensorRepository, times(4)).findByDeviceIdAndLastUpdated(any(), any());
        verify(thermostatRepository, times(3)).findByDeviceIdAndLastUpdated(any(), any());
        verify(smartPlugRepository, times(5)).findByDeviceIdAndLastUpdated(any(), any());
        verify(retryProperties, times(12)).getMaxAttempts();
        verify(deadLetterProducer).send(List.of(thermostatEvent3), LongStream.rangeClosed(0, 11).boxed().collect(toSet()));
        verify(kpiMetricLogger).incNonRetriableSkippedErrorsCount(anyString());
    }
}