package net.acilab.stream.processor.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.acilab.stream.configuration.WikiLinksKafkaAppConfig;
import net.acilab.stream.configuration.WikiLinksKafkaSpringAppConfiguration;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

@Service
public class WikiLinksEnrichmentSimpleConsumer implements WikiLinksEnrichmentConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksEnrichmentSimpleConsumer.class);

  @Inject
  private WikiLinksKafkaSpringAppConfiguration appConfig;

  @Override
  public void run() {

    LOGGER.info("=== Starting WikiLinksEnrichmentThroughputConsumer ===");
    LOGGER.info("Configuration is {}", appConfig.getKafkaConsumerConfiguration().toString());

    KafkaConsumer<String, WikiLinksArticleEvent> consumer = new KafkaConsumer<>(
        appConfig.getKafkaConsumerConfiguration());

    Thread haltedHook = new Thread(consumer::close);
    Runtime.getRuntime().addShutdownHook(haltedHook);

    // consumer.subscribe(Collections.singletonList(appConfig.getKafkaProducerTopic()));

    TopicPartition partition0 = new TopicPartition(appConfig.getKafkaProducerTopic(), 0);
    TopicPartition partition1 = new TopicPartition(appConfig.getKafkaProducerTopic(), 1);
    TopicPartition partition2 = new TopicPartition(appConfig.getKafkaProducerTopic(), 2);

    OffsetAndMetadata partition0Offset = new OffsetAndMetadata(0L);
    OffsetAndMetadata partition1Offset = new OffsetAndMetadata(0L);
    OffsetAndMetadata partition2Offset = new OffsetAndMetadata(0L);

    consumer.assign(List.of(partition0, partition1, partition2));

    consumer.seek(partition0, partition0Offset);
    consumer.seek(partition1, partition1Offset);
    consumer.seek(partition2, partition2Offset);

    while (true) {
      ConsumerRecords<String, WikiLinksArticleEvent> events = consumer.poll(Duration.ofMillis(100));
      events.forEach(event -> processEvent(event.value()));
    }
  }

  private static void processEvent(WikiLinksArticleEvent event) {
    LOGGER.info("Event processed: {}", event.getUrl());
    LOGGER.info("Finsh processing.");
  }

}
