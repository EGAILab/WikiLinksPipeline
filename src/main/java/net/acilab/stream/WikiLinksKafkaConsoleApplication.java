package net.acilab.stream;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.controller.StreamController;
import net.acilab.stream.controller.WikiLinksEnrichmentController;
import net.acilab.stream.controller.WikiLinksKafkaController;

public class WikiLinksKafkaConsoleApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaConsoleApplication.class);

  private static final StreamController producerController = new WikiLinksKafkaController();
  private static final StreamController consumerController = new WikiLinksEnrichmentController();

  @Inject
  public WikiLinksKafkaConsoleApplication() {
  }

  public static void main(String[] args) {
    LOGGER.info("=== Starting WikiLinksKafkaConsoleApplication ===");
    // producerController.initializeStream();
    consumerController.initializeStream();
  }
}
