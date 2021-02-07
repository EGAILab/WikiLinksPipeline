package net.acilab.stream.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.acilab.stream.processor.kafka.WikiLinksEnrichmentConsumer;
import net.acilab.stream.processor.kafka.WikiLinksEnrichmentSimpleConsumer;

@Service
public class WikiLinksEnrichmentSimpleController implements StreamController {

  @Autowired
  private WikiLinksEnrichmentSimpleConsumer consumer;

  public void initializeStream() {
    consumer.run();
  }
}
