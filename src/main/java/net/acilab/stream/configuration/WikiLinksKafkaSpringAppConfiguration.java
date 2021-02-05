package net.acilab.stream.configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Qualifier("Spring")
@Configuration("WikiLinksKafkaSpringConfiguration")
@PropertySource("classpath:application.properties")
public class WikiLinksKafkaSpringAppConfiguration implements WikiLinksKafkaAppConfiguration {

  // Kafka properties

  @Value("${kafka.bootstrap.servers}")
  private String kafkaBootStrapServers;

  @Value("${kafka.schema.registry.url}")
  private String kafkaSchemaRegistryUrl;

  @Value("${kafka.producer.topic}")
  private String kafkaProducerTopic;

  @Value("${kafka.producer.thread.pool.size}")
  private String kafkaProducerThreadPoolSize;

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
