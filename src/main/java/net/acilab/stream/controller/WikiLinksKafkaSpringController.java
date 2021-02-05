package net.acilab.stream.controller;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import net.acilab.stream.configuration.WikiLinksKafkaAppConfiguration;

//@Service
public class WikiLinksKafkaSpringController extends WikiLinksKafkaController {

  @Inject
  public WikiLinksKafkaSpringController(@Qualifier("Spring") WikiLinksKafkaAppConfiguration appConfig) {
    super(appConfig);
  }
}
