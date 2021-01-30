package net.acilab.stream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import net.acilab.stream.controller.ProducerController;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication(scanBasePackages = "net.acilab.stream")
public class WikiLinksKafkaStreamApplication implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaStreamApplication.class);

	private ProducerController producerController;

	@Inject
	public WikiLinksKafkaStreamApplication(ProducerController producerController) {
		this.producerController = producerController;
	}

	@Override
	public void run(String... args) throws Exception {
		producerController.initializeKafkaProducer();
	}

	public static void main(String[] args) {
		LOGGER.info("=== Starting WikiLinksKafkaStreamApplication ===");
		SpringApplication.run(WikiLinksKafkaStreamApplication.class, args);
	}
}
