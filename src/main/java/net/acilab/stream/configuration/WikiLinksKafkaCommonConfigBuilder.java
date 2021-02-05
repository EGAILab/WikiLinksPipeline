package net.acilab.stream.configuration;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.acilab.stream.processor.kafka.configuration.KafkaProducerConfigBuilder;
import net.acilab.stream.processor.wikilinks.configuration.EventFileConfigBuilder;

public class WikiLinksKafkaCommonConfigBuilder
    implements EventFileConfigBuilder, KafkaProducerConfigBuilder, WikiLinksAppConfig, KafkaProducerAppConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaCommonConfigBuilder.class);

  // Producer properties
  private String kafkaBootStrapServers;
  private String kafkaStreamingSchemaRegistry;
  private String topic;
  private String threadPoolSize;

  // Event File Properties
  private String eventFileLocation;
  private String eventPointerFileSuffix;
  private String eventFileNames;
  private String eventFileNumber;
  private String eventBatchSize;

  public WikiLinksKafkaCommonConfigBuilder() {
    Configurations configBuilder = new Configurations();

    try {
      Configuration kafkaConfig = configBuilder.properties(new File("kafka.properties"));

      kafkaBootStrapServers = kafkaConfig.getString("kafka.bootstrap.server");
      kafkaStreamingSchemaRegistry = kafkaConfig.getString("kafka.schema.registry.url");
      topic = kafkaConfig.getString("producer.topic");
      threadPoolSize = kafkaConfig.getString("producer.thread.pool.size");

      Configuration eventFileConfig = configBuilder.properties(new File("wikilinks.properties"));

      eventFileLocation = eventFileConfig.getString("event.file.location");
      eventPointerFileSuffix = eventFileConfig.getString("event.pointer.file.suffix");
      eventFileNames = eventFileConfig.getString("event.file.names");
      eventFileNumber = eventFileConfig.getString("event.file.number");
      eventBatchSize = eventFileConfig.getString("event.file.batch.size");

    } catch (ConfigurationException cex) {
      LOGGER.error("Unable to open configuration file. Error is: {}", cex);
      cex.printStackTrace();
      System.exit(0);
    }
  }

  /* Producer Configs */

  public String getKafkaProducerTopic() {
    return topic;
  }

  public int getKafkaProducerThreadPoolSize() {
    return Integer.parseInt(threadPoolSize);
  }

  public Properties getKafkaThroughputProducerConfiguration() {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaStreamingSchemaRegistry);
    // High Throughput config
    config.put(ProducerConfig.ACKS_CONFIG, "0");
    config.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    config.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");
    return config;
  }

  /* Event File Configs */

  public String getEventFileLocation() {
    return eventFileLocation;
  }

  public List<String> getEventFileNames() {
    return Arrays.asList(eventFileNames.split(","));
  }

  public List<String> getEventFiles() {
    List<String> fileNameList = getEventFileNames();
    List<String> eventFileList = fileNameList.stream().map(s -> eventFileLocation + s).collect(Collectors.toList());
    return eventFileList;
  }

  public List<String> getEventPointerFiles() {
    List<String> eventFileList = getEventFiles();
    List<String> eventPointerFileList = eventFileList.stream().map(s -> s + eventPointerFileSuffix)
        .collect(Collectors.toList());
    return eventPointerFileList;
  }

  public int getEventFileTotal() {
    return Integer.parseInt(eventFileNumber);
  }

  public int getEventFileReadBatchSize() {
    return Integer.parseInt(eventBatchSize);
  }
}
