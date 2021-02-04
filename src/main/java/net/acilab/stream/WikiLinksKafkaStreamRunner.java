package net.acilab.stream;

import net.acilab.stream.configuration.WikiLinksKafkaAppConfigBuilder;
import net.acilab.stream.processor.kafka.WikiLinksKafkaThroughputProducer;
import net.acilab.stream.processor.wikilinks.WikiLinksEventFileProcessor;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

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

public class WikiLinksKafkaStreamRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaStreamApplication.class);

  private static final WikiLinksKafkaAppConfigBuilder appConfigBuilder = new WikiLinksKafkaAppConfigBuilder();
  private static final String topic = appConfigBuilder.getTopic();
  private static final int batchSize = appConfigBuilder.getBatchSize();
  private static final int threadPoolSize = appConfigBuilder.getThreadPoolSize();
  private static boolean runOnce = false;

  public static void main(String[] args) {

    LOGGER.info("=== Starting WikiLinksKafkaStreamRunner ===");

    final Producer<String, WikiLinksArticleEvent> producer = createProducer();
    final List<WikiLinksKafkaThroughputProducer> producerList = getProducerList(producer);

    // final ExecutorService executorService =
    // Executors.newFixedThreadPool(threadPoolSize + 1);
    // executorService.submit(new MetricsProducerReporter(producer));
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

  private static Producer<String, WikiLinksArticleEvent> createProducer() {
    return new KafkaProducer<>(appConfigBuilder.getProducerConfiguration());
  }

  private static WikiLinksEventFileProcessor createEventFileProcessor() {
    return new WikiLinksEventFileProcessor(appConfigBuilder);
  }

  private static List<WikiLinksKafkaThroughputProducer> getProducerList(
      final Producer<String, WikiLinksArticleEvent> producer) {
    return Arrays.asList(
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 0, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 1, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 2, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 3, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 4, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 5, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 6, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 7, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 8, batchSize, runOnce),
        new WikiLinksKafkaThroughputProducer(producer, createEventFileProcessor(), topic, 9, batchSize, runOnce));
  }
}
