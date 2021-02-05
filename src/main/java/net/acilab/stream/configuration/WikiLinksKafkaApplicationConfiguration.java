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

public class WikiLinksKafkaApplicationConfiguration implements WikiLinksAppConfig, KafkaProducerAppConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaApplicationConfiguration.class);

  private static volatile WikiLinksKafkaApplicationConfiguration configurationInstance = null;

  // Kafka properties
  private final String kafkaBootStrapServers;
  private final String kafkaSchemaRegistryUrl;
  private final String kafkaProducerTopic;
  private final String kafkaProducerThreadPoolSize;

  // Event file properties
  private final String eventFileLocation;
  private final String eventFilePointerFileSuffix;
  private final String eventFileNames;
  private final String eventFileTotal;
  private final String eventFileReadBatchSize;

  private WikiLinksKafkaApplicationConfiguration() {

    if (configurationInstance != null) {
      throw new RuntimeException("Use getConfiguration() to create configuration.");
    }

    Configurations configBuilder = new Configurations();
    Configuration configuration = null;
    try {
      configuration = configBuilder.properties(new File("application.properties"));

    } catch (ConfigurationException cex) {
      LOGGER.error("Unable to open configuration file. Error is: {}", cex);
      cex.printStackTrace();
      System.exit(0);
    }

    kafkaBootStrapServers = configuration.getString("kafka.bootstrap.servers");
    kafkaSchemaRegistryUrl = configuration.getString("kafka.schema.registry.url");
    kafkaProducerTopic = configuration.getString("kafka.producer.topic");
    kafkaProducerThreadPoolSize = configuration.getString("kafka.producer.thread.pool.size");

    eventFileLocation = configuration.getString("event.file.location");
    eventFilePointerFileSuffix = configuration.getString("event.file.pointer.file.suffix");
    eventFileNames = configuration.getString("event.file.names");
    eventFileTotal = configuration.getString("event.file.total");
    eventFileReadBatchSize = configuration.getString("event.file.read.batch.size");
  }

  public static WikiLinksKafkaApplicationConfiguration getConfiguration() {
    if (configurationInstance == null) {
      synchronized (WikiLinksKafkaApplicationConfiguration.class) {
        if (configurationInstance == null) {
          configurationInstance = new WikiLinksKafkaApplicationConfiguration();
        }
      }
    }
    return configurationInstance;
  }

  // Kafka getters

  public Properties getKafkaThroughputProducerConfiguration() {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);
    // High Throughput config
    config.put(ProducerConfig.ACKS_CONFIG, "0");
    config.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    config.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");
    return config;
  }

  public String getKafkaProducerTopic() {
    return kafkaProducerTopic;
  }

  public int getKafkaProducerThreadPoolSize() {
    return Integer.parseInt(kafkaProducerThreadPoolSize);
  }

  // Event file getters

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
    List<String> eventFiles = getEventFiles();
    List<String> eventPointerFileList = eventFiles.stream().map(s -> s + eventFilePointerFileSuffix)
        .collect(Collectors.toList());
    return eventPointerFileList;
  }

  public int getEventFileTotal() {
    return Integer.parseInt(eventFileTotal);
  }

  public int getEventFileReadBatchSize() {
    return Integer.parseInt(eventFileReadBatchSize);
  }
}
