package net.acilab.stream.processor.kafka;

import org.springframework.stereotype.Service;

import net.acilab.stream.processor.wikilinks.EventFileProcessor;

import javax.inject.Inject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class WikiLinksKafkaProducerRunable implements ProducerRunable, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaProducerRunable.class);

  EventFileProcessor eventFileProcessor;

  private KafkaProducer<Integer, String> producer;
  private String topic;
  private int fileIndex;

  @Inject
  void setEventFileProcessor(EventFileProcessor eventFileProcessor) {
    this.eventFileProcessor = eventFileProcessor;
  }

  public void prepareProducer(KafkaProducer<Integer, String> producer, String topic, int fileIndex) {
    this.producer = producer;
    this.topic = topic;
    this.fileIndex = fileIndex;
  }

  @Override
  public void run() {
    LOGGER.info("=== Starting WikiLinksKafkaProducerRunable ===");

    LOGGER.info("Producer Topic is: {}", topic);
    LOGGER.info("Producer is: {}", producer.toString());
    LOGGER.info("Closing producer ...");
    producer.close();
    LOGGER.info("Producer closed.");

    eventFileProcessor.readNextEvent(this.fileIndex, 1);
  }
}
