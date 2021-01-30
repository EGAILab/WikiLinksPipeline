package net.acilab.stream.controller;

import java.util.Properties;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import net.acilab.stream.processor.kafka.WikiLinksKafkaProducerRunable;
import net.acilab.stream.processor.kafka.configuration.KafkaProducerThroughputConfigBuilder;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class WikiLinksKafkaProducerController implements ProducerController {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaProducerController.class);

  private KafkaProducerThroughputConfigBuilder producerConfigBuilder;
  private WikiLinksKafkaProducerRunable producerRunable;

  @Inject
  public WikiLinksKafkaProducerController(KafkaProducerThroughputConfigBuilder producerConfigBuilder,
      WikiLinksKafkaProducerRunable producerRunable) {
    this.producerConfigBuilder = producerConfigBuilder;
    this.producerRunable = producerRunable;
  }

  @Override
  public void initializeKafkaProducer() {
    LOGGER.info("=== Starting WikiLinksKafkaProducerController ===");

    Properties producerConfig = producerConfigBuilder.getProducerConfiguration();
    LOGGER.info("Producer Configuration is: {}", producerConfig);
    String topic = producerConfigBuilder.getTopic();
    KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerConfig);
    int fileIndex = 0;

    producerRunable.prepareProducer(producer, topic, fileIndex);
    producerRunable.run();
  }
}
