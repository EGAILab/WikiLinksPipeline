package net.acilab.stream.controller;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import net.acilab.stream.configuration.WikiLinksKafkaAppConfig;
import net.acilab.stream.configuration.WikiLinksKafkaApplicationConfiguration;
import net.acilab.stream.processor.kafka.WikiLinksKafkaThroughputProducerRunable;
import net.acilab.stream.processor.wikilinks.WikiLinksEventFileProcessor;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

@Service
public class WikiLinksKafkaController implements StreamController {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaController.class);
  private static final WikiLinksKafkaAppConfig appConfig = WikiLinksKafkaApplicationConfiguration
      .getConfiguration();

  private final String topic = appConfig.getKafkaProducerTopic();
  private final int batchSize = appConfig.getEventFileReadBatchSize();
  private final int threadPoolSize = appConfig.getKafkaProducerThreadPoolSize();
  private final boolean runOnce = false;

  public void initializeStream() {

    LOGGER.info("=== Starting WikiLinksKafkaSpringConsoleController ===");

    final Producer<String, WikiLinksArticleEvent> producer = createProducer();
    final List<WikiLinksKafkaThroughputProducerRunable> producerList = getProducerList(producer);

    final ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    producerList.forEach(executorService::submit);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      executorService.shutdown();
      try {
        executorService.awaitTermination(200, TimeUnit.MILLISECONDS);
        LOGGER.info("Flushing and closing producer");
        producer.flush();
        producer.close(Duration.ofMillis(10_000));
      } catch (InterruptedException e) {
        LOGGER.warn("shutting down", e);
      }
    }));
  }

  private Producer<String, WikiLinksArticleEvent> createProducer() {
    return new KafkaProducer<>(appConfig.getKafkaThroughputProducerConfiguration());
  }

  private WikiLinksEventFileProcessor createEventFileProcessor() {
    return new WikiLinksEventFileProcessor();
  }

  private List<WikiLinksKafkaThroughputProducerRunable> getProducerList(
      final Producer<String, WikiLinksArticleEvent> producer) {
    return Arrays.asList(
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 0, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 1, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 2, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 3, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 4, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 5, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 6, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 7, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 8, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducerRunable(producer, createEventFileProcessor(), topic, 9, batchSize,
            runOnce));
  }
}
