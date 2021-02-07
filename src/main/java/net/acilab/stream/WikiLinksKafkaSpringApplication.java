package net.acilab.stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import net.acilab.stream.configuration.WikiLinksKafkaSpringAppConfiguration;
import net.acilab.stream.controller.StreamController;
import net.acilab.stream.controller.WikiLinksEnrichmentSimpleController;
import net.acilab.stream.controller.WikiLinksKafkaController;
import net.acilab.stream.controller.WikiLinksKafkaSpringController;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication(scanBasePackages = "net.acilab.stream")
public class WikiLinksKafkaSpringApplication implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaSpringApplication.class);

	// Spring Producer
	@Autowired
	private WikiLinksKafkaSpringAppConfiguration appConfig;

	@Autowired
	private WikiLinksEnrichmentSimpleController consumerController;

	@Override
	public void run(String... args) throws Exception {
		// Spring Producer
		// StreamController controller = new WikiLinksKafkaSpringController(appConfig);
		// Console Producer
		// StreamController controller = new WikiLinksKafkaController();
		// producerController.initializeStream();

		consumerController.initializeStream();
	}

	public static void main(String[] args) {
		LOGGER.info("=== Starting WikiLinksKafkaStreamApplication ===");
		SpringApplication.run(WikiLinksKafkaSpringApplication.class, args);
	}
}
