package net.acilab.stream.controller;

import net.acilab.stream.configuration.WikiLinksKafkaAppConfiguration;

public class WikiLinksKafkaConsoleController extends WikiLinksKafkaController {

  public WikiLinksKafkaConsoleController(final WikiLinksKafkaAppConfiguration appConfig) {
    super(appConfig);
  }
}
