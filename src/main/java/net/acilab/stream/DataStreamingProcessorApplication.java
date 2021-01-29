package net.acilab.stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class DataStreamingProcessorApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamingProcessorApplication.class);

	public static void main(String[] args) {
		LOGGER.info("=== Starting DataStreamingProcessorApplication ===");
		SpringApplication.run(DataStreamingProcessorApplication.class, args);
	}

}
