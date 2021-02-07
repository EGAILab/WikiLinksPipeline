package net.acilab.stream.processor.kafka;

import java.time.Duration;
import java.util.Collections;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

public class WikiLinksEnrichmentThroughputConsumerRunable implements WikiLinksEnrichmentConsumer, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksEnrichmentThroughputConsumerRunable.class);

  private final Consumer<String, WikiLinksArticleEvent> consumer;
  private final String topic;

  @Inject
  public WikiLinksEnrichmentThroughputConsumerRunable(Consumer<String, WikiLinksArticleEvent> consumer, String topic) {
    this.consumer = consumer;
    this.topic = topic;
  }

  @Override
  public void run() {

    LOGGER.info("=== Starting WikiLinksEnrichmentThroughputConsumerRunable ===");
    LOGGER.info("Consuming Topic: {}", topic);

    consumer.subscribe(Collections.singletonList(topic));
    try {
      while (true) {
        ConsumerRecords<String, WikiLinksArticleEvent> events = consumer.poll(Duration.ofMillis(200));
        events.forEach(event -> processEvent(event.value()));
      }
    } catch (WakeupException we) {
      LOGGER.error("Wake up received.");
    }
  }

  private static void processEvent(WikiLinksArticleEvent event) {
    LOGGER.info("Event processed: {}", event.getUrl());
    LOGGER.info("Finsh processing.");
  }
}
