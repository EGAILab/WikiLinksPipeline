package net.acilab.stream;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.configuration.WikiLinksKafkaConsoleAppConfiguration;
import net.acilab.stream.controller.StreamController;
import net.acilab.stream.controller.WikiLinksKafkaConsoleController;

public class WikiLinksKafkaConsoleApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaConsoleApplication.class);
  private static final WikiLinksKafkaConsoleAppConfiguration appConfig = WikiLinksKafkaConsoleAppConfiguration
      .getConfiguration();
  private static final StreamController streamController = new WikiLinksKafkaConsoleController(appConfig);

  @Inject
  public WikiLinksKafkaConsoleApplication() {
  }

  public static void main(String[] args) {
    LOGGER.info("=== Starting WikiLinksKafkaConsoleApplication ===");
    streamController.initializeStream();
  }
}
