package com.iot.devices.management.telemetry_ingestion_persister.kafka;

import com.iot.devices.*;
import com.iot.devices.management.telemetry_ingestion_persister.health.HealthConfig;
import com.iot.devices.management.telemetry_ingestion_persister.kafka.properties.KafkaConsumerProperties;
import com.iot.devices.management.telemetry_ingestion_persister.persictence.TelemetryPersister;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.KafkaContainer; //TODO: migrate to .kafka. instead of containers!!!
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.iot.devices.DoorState.OPEN;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.timeout;

@Slf4j
@ActiveProfiles("test")
@SpringBootTest(
        classes = {
                KafkaConsumerRunner.class,
                KafkaConsumerConfig.class,
                TestKafkaProducer.class,
                KafkaProducerProperties.class,
                KafkaConsumerProperties.class,
                HealthConfig.class,
                SimpleMeterRegistry.class,
                MockClock.class
        },
        properties = "classpath:application-test.yaml")
@Testcontainers
class KafkaConsumerRunnerTest {

    @MockitoBean
    TelemetryPersister persister;

    @Autowired
    KafkaConsumerProperties consumerProperties;
    @Autowired
    TestKafkaProducer kafkaProducer;

    @Captor
    ArgumentCaptor<List<ConsumerRecord<String, SpecificRecord>>> recordsCaptor;

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("kafka.consumer.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
        registry.add("kafka.producer.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
    }

    @BeforeAll
    static void start() {
        kafkaContainer.start();
    }

    @AfterAll
    static void close() {
        kafkaContainer.close();
    }

    @AfterEach
    void tearDown() throws ExecutionException, InterruptedException {
        reset(persister);
    }

    @Test
    void successfulMessageTransfer() {
        String deviceId1 = UUID.randomUUID().toString();
        String deviceId2 = UUID.randomUUID().toString();
        String deviceId3 = UUID.randomUUID().toString();

        Instant nowTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        DoorSensor doorSensor = new DoorSensor(deviceId1, OPEN, 85, false,
                DeviceStatus.OFFLINE, nowTime, "1.0.2v", nowTime);

        Thermostat thermostat = new Thermostat(deviceId2, 26.6f, 24.0f, 10.0f,
                ThermostatMode.COOL, DeviceStatus.ONLINE, "2.123v", nowTime);

        SmartPlug smartPlug = new SmartPlug(deviceId3, true, 230f, 227f, 99f,
                DeviceStatus.MAINTENANCE, null, nowTime.minus(5, ChronoUnit.MINUTES));

        kafkaProducer.sendMessage(doorSensor, deviceId1);
        kafkaProducer.sendMessage(thermostat, deviceId2);
        kafkaProducer.sendMessage(smartPlug, deviceId3);

        verify(persister, timeout(3000)).persist(recordsCaptor.capture());
        List<List<ConsumerRecord<String, SpecificRecord>>> receivedMessages = recordsCaptor.getAllValues();

//        assertEquals(3, receivedMessages.getFirst().size());
//        assertEquals(doorSensor, receivedMessages.getFirst().get(deviceId1).value());
//        assertEquals(thermostat, receivedMessages.getFirst().get(deviceId2).value());
//        assertEquals(smartPlug, receivedMessages.getFirst().get(deviceId3).value());
    }

    @Test
    void sendAfterRetries() {
        String deviceId1 = UUID.randomUUID().toString();
        String deviceId2 = UUID.randomUUID().toString();
        String deviceId3 = UUID.randomUUID().toString();

        List<String> deviceIds = asList(deviceId1, deviceId2, deviceId3);

        when(persister.persist(any())).thenThrow(
                new RuntimeException("Something bad happened 1"),
                new RuntimeException("Something bad happened 2"),
                new RuntimeException("Something bad happened 3"));

        Instant nowTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        DoorSensor doorSensor = new DoorSensor(deviceId1, OPEN, 85, false,
                DeviceStatus.OFFLINE, nowTime, "1.0.2v", nowTime);

        Thermostat thermostat = new Thermostat(deviceId2, 26.6f, 24.0f, 10.0f,
                ThermostatMode.COOL, DeviceStatus.ONLINE, "2.123v", nowTime);

        SmartPlug smartPlug = new SmartPlug(deviceId3, true, 230f, 227f, 99f,
                DeviceStatus.MAINTENANCE, null, nowTime.minus(5, ChronoUnit.MINUTES));

        kafkaProducer.sendMessage(doorSensor, deviceId1);
        kafkaProducer.sendMessage(thermostat, deviceId2);
        kafkaProducer.sendMessage(smartPlug, deviceId3);

        verify(persister, timeout(30000).atLeast(4)).persist(recordsCaptor.capture());
        List<List<ConsumerRecord<String, SpecificRecord>>> receivedMessages = recordsCaptor.getAllValues();

//        Map<String, SpecificRecord> messageById = receivedMessages.stream().map(Map::values).flatMap(Collection::stream)
//                .filter(x -> deviceIds.contains(x.key()))
//                .collect(Collectors.toMap(ConsumerRecord::key, ConsumerRecord::value, (a, b) -> b));
//        assertEquals(3, messageById.size());
//        assertEquals(doorSensor, messageById.get(deviceId1));
//        assertEquals(thermostat, messageById.get(deviceId2));
//        assertEquals(smartPlug, messageById.get(deviceId3));
    }
}