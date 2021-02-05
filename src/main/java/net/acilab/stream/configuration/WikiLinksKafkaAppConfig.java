package net.acilab.stream.configuration;

import java.util.List;
import java.util.Properties;

public interface WikiLinksKafkaAppConfig {

  public String getKafkaProducerTopic();

  public int getKafkaProducerThreadPoolSize();

  public Properties getKafkaThroughputProducerConfiguration();

  public String getEventFileLocation();

  public List<String> getEventFileNames();

  public List<String> getEventFiles();

  public List<String> getEventPointerFiles();

  public int getEventFileTotal();

  public int getEventFileReadBatchSize();
}
