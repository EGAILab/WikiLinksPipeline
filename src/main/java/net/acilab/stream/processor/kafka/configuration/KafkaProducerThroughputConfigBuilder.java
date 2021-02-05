package net.acilab.stream.processor.kafka.configuration;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.acilab.stream.configuration.KafkaProducerAppConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration("KafkaProducerThroughputConfigBuilder")
@PropertySource("classpath:kafka.properties")
public class KafkaProducerThroughputConfigBuilder implements KafkaProducerConfigBuilder, KafkaProducerAppConfig {

  @Value("${kafka.bootstrap.server}")
  private String kafkaBootStrapServers;

  @Value("${kafka.schema.registry.url}")
  private String kafkaStreamingSchemaRegistry;

  @Value("${producer.topic}")
  private String topic;

  @Value("${producer.thread.pool.size}")
  private String threadPoolSize;

  public String getKafkaProducerTopic() {
    return topic;
  }

  public int getKafkaProducerThreadPoolSize() {
    return Integer.parseInt(threadPoolSize);
  }

  @Override
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
}
