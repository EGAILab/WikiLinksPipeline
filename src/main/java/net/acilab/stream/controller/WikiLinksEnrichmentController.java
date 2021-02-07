package net.acilab.stream.controller;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.configuration.WikiLinksKafkaAppConfig;
import net.acilab.stream.configuration.WikiLinksKafkaApplicationConfiguration;
import net.acilab.stream.processor.kafka.WikiLinksEnrichmentThroughputConsumerRunable;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

public class WikiLinksEnrichmentController implements StreamController {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksEnrichmentController.class);
  private static final WikiLinksKafkaAppConfig appConfig = WikiLinksKafkaApplicationConfiguration.getConfiguration();

  private final String topic = appConfig.getKafkaProducerTopic();

  public void initializeStream() {

    LOGGER.info("=== Starting WikiLinksEnrichmentController ===");

    final Consumer<String, WikiLinksArticleEvent> consumer = new KafkaConsumer<>(
        appConfig.getKafkaConsumerConfiguration());
    final List<WikiLinksEnrichmentThroughputConsumerRunable> consumerList = getConsumerList(consumer);

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    consumerList.forEach(executorService::submit);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      executorService.shutdown();
      try {
        executorService.awaitTermination(200, TimeUnit.MILLISECONDS);
        LOGGER.info("Closing consumer");
        consumer.close(Duration.ofMillis(10_000));
      } catch (InterruptedException e) {
        LOGGER.warn("shutting down", e);
      }
    }));
  }

  private List<WikiLinksEnrichmentThroughputConsumerRunable> getConsumerList(
      final Consumer<String, WikiLinksArticleEvent> consumer) {
    return Arrays.asList(new WikiLinksEnrichmentThroughputConsumerRunable(consumer, topic));
  }
}
