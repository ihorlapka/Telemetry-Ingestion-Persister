package com.iot.devices.management.telemetry_ingestion_persister;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class TelemetryIngestionPersisterApplicationTests {

	@Test
	void contextLoads() {
	}

}
