package net.acilab.stream.configuration;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

@Component
public class WikiLinksKafkaApplicationConfiguration implements WikiLinksKafkaAppConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaApplicationConfiguration.class);

  private static volatile WikiLinksKafkaApplicationConfiguration configurationInstance = null;

  // Kafka properties
  private final String kafkaBootStrapServers;
  private final String kafkaSchemaRegistryUrl;
  private final String kafkaProducerTopic;
  private final String kafkaProducerThreadPoolSize;
  private final String kafkaConsumerMaxPoolRecords;
  private final String kafkaConsumerNumStreamThreads;

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
    kafkaConsumerMaxPoolRecords = configuration.getString("kafka.consumer.max.pool.records");
    kafkaConsumerNumStreamThreads = configuration.getString("kafka.consumer.num.stream.threads");

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

  public Properties getKafkaConsumerConfiguration() {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);
    config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    // Specific config
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConsumerMaxPoolRecords);

    return config;
  }

  // not working
  public Properties getKafkaConsumerConfiguration_CBA() {
    Properties config = new Properties();
    // config.put(StreamsConfig.APPLICATION_ID_CONFIG, "");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    // config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
    // kafkaSchemaRegistryUrl);
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);
    config.put(StreamsConfig.CONSUMER_PREFIX + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        kafkaSchemaRegistryUrl);
    // config.put(StreamsConfig.CONSUMER_PREFIX +
    // ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
    // "au.com.commbank.kafka.toolkit.logging.client.KafkaConsumerInterceptor");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroDeserializer.class);
    config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaConsumerNumStreamThreads);
    // config.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    // config.put(StreamsConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
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
