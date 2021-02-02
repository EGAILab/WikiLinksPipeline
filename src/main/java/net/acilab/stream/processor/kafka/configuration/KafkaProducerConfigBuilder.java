package net.acilab.stream.processor.kafka.configuration;

import java.util.Properties;

public interface KafkaProducerConfigBuilder {

  String getTopic();

  int getThreadPoolSize();

  Properties getProducerConfiguration();
}
