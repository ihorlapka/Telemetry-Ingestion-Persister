package com.iot.devices.management.telemetry_ingestion_persister;

import org.springframework.boot.SpringApplication;

public class TestTelemetryIngestionPersisterApplication {

	public static void main(String[] args) {
		SpringApplication.from(TelemetryIngestionPersisterApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
