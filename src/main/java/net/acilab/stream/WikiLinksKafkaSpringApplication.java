package net.acilab.stream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import net.acilab.stream.controller.StreamController;
import net.acilab.stream.controller.WikiLinksKafkaStreamController;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication(scanBasePackages = "net.acilab.stream")
public class WikiLinksKafkaSpringApplication implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaSpringApplication.class);

	private StreamController controller;

	@Inject
	public WikiLinksKafkaSpringApplication(WikiLinksKafkaStreamController controller) {
		this.controller = controller;
	}

	@Override
	public void run(String... args) throws Exception {
		controller.initializeStream();
	}

	public static void main(String[] args) {
		LOGGER.info("=== Starting WikiLinksKafkaStreamApplication ===");
		SpringApplication.run(WikiLinksKafkaSpringApplication.class, args);
	}
}
