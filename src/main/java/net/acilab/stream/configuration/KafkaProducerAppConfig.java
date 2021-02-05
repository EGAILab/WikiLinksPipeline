package net.acilab.stream.configuration;

import java.util.Properties;

public interface KafkaProducerAppConfig {

  public String getKafkaProducerTopic();

  public int getKafkaProducerThreadPoolSize();

  public Properties getKafkaThroughputProducerConfiguration();
}