package net.acilab.stream.controller;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import net.acilab.stream.processor.kafka.WikiLinksKafkaProducerRunable;
import net.acilab.stream.processor.kafka.configuration.KafkaProducerConfigBuilder;
import net.acilab.stream.processor.kafka.configuration.KafkaProducerThroughputConfigBuilder;
import net.acilab.stream.processor.wikilinks.configuration.WikiLinksEventFileConfigBuilder;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class WikiLinksKafkaProducerController implements ProducerController {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaProducerController.class);

  private static final int BATCH_SIZE = 3;

  private KafkaProducerConfigBuilder producerConfigBuilder;
  private WikiLinksEventFileConfigBuilder fileConfigBuilder;
  private WikiLinksKafkaProducerRunable producerRunable;

  @Inject
  public WikiLinksKafkaProducerController(KafkaProducerThroughputConfigBuilder producerConfigBuilder,
      WikiLinksEventFileConfigBuilder fileConfigBuilder, WikiLinksKafkaProducerRunable producerRunable) {
    this.producerConfigBuilder = producerConfigBuilder;
    this.fileConfigBuilder = fileConfigBuilder;
    this.producerRunable = producerRunable;
  }

  @Override
  public void initializeProducer() {
    LOGGER.info("=== Starting WikiLinksKafkaProducerController ===");

    Properties producerConfig = producerConfigBuilder.getProducerConfiguration();
    // LOGGER.info("Producer Configuration is: {}", producerConfig);
    String topic = producerConfigBuilder.getTopic();
    LOGGER.info("Topic is: {}", topic);
    int threadPoolSize = producerConfigBuilder.getThreadPoolSize();
    LOGGER.info("Thread Pool Size is: {}", threadPoolSize);
    KafkaProducer<String, WikiLinksArticleEvent> producer = new KafkaProducer<>(producerConfig);
    int numberOfFiles = fileConfigBuilder.getEventFileNumber();
    LOGGER.info("Number of file: {}", numberOfFiles);
    int fileIndex = 0;

    producerRunable.prepareProducer(producer, topic, fileIndex, BATCH_SIZE, false);
    producerRunable.run();

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);
    for (int i = 0; i < numberOfFiles; i++) {
      fileIndex = i;
      executor.submit(() -> {
      });
    }
  }
}
