package net.acilab.stream;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

@Configuration("TestConfigBuilder")
@PropertySource("classpath:application-test.properties")
public class TestConfigBuilder {

  @Value("${event.file.location}")
  private String eventFileLocation;

  @Value("${event.file.names}")
  private String eventFileNames;

  @Value("${event.pointer.file.suffix}")
  private String eventPointerFileSuffix;

  @Value("${kafka.bootstrap.server}")
  private String kafkaBootStrapServers;

  @Value("${kafka.schema.registry.url}")
  private String kafkaStreamingSchemaRegistry;

  @Value("${producer.topic}")
  private String topic;

  public String getEventFileLocation() {
    return eventFileLocation;
  }

  public List<String> getEventFileNames() {
    return Arrays.asList(eventFileNames.split(","));
  }

  public List<String> getEventFileList() {
    List<String> fileNameList = getEventFileNames();
    List<String> eventFileList = fileNameList.stream().map(s -> eventFileLocation + s).collect(Collectors.toList());
    return eventFileList;
  }

  public List<String> getEventPointerFileList() {
    List<String> eventFileList = getEventFileList();
    List<String> eventPointerFileList = eventFileList.stream().map(s -> s + eventPointerFileSuffix)
        .collect(Collectors.toList());
    return eventPointerFileList;
  }

  public String getTopic() {
    return topic;
  }

  public Properties getProducerConfiguration() {
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
}
