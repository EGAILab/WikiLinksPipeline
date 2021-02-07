package net.acilab.stream.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.acilab.stream.processor.kafka.WikiLinksEnrichmentConsumer;

@Service
public class WikiLinksEnrichmentController implements StreamController {

  @Autowired
  private WikiLinksEnrichmentConsumer consumer;

  public void initializeStream() {
    consumer.run();
  }
}
