package net.acilab.stream.configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Qualifier("Spring")
@Configuration("WikiLinksKafkaSpringConfiguration")
@PropertySource("classpath:application.properties")
public class WikiLinksKafkaSpringAppConfiguration implements WikiLinksKafkaAppConfig {

  // Kafka properties

  @Value("${kafka.bootstrap.servers}")
  private String kafkaBootStrapServers;

  @Value("${kafka.schema.registry.url}")
  private String kafkaSchemaRegistryUrl;

  @Value("${kafka.producer.topic}")
  private String kafkaProducerTopic;

  @Value("${kafka.producer.thread.pool.size}")
  private String kafkaProducerThreadPoolSize;

  @Value("${kafka.consumer.max.pool.records}")
  private String kafkaConsumerMaxPoolRecords;

  @Value("${kafka.consumer.num.stream.threads}")
  private String kafkaConsumerNumStreamThreads;

  // Event file properties

  @Value("${event.file.location}")
  private String eventFileLocation;

  @Value("${event.file.pointer.file.suffix}")
  private String eventFilePointerFileSuffix;

  @Value("${event.file.names}")
  private String eventFileNames;

  @Value("${event.file.total}")
  private String eventFileTotal;

  @Value("${event.file.read.batch.size}")
  private String eventFileReadBatchSize;

  public WikiLinksKafkaSpringAppConfiguration() {

  }

  // Kafka getters

  public Properties getKafkaThroughputProducerConfiguration() {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);
    // Specific config
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
