package net.acilab.stream.configuration;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.TestDataFactory;

@Ignore
public class ApplicationConfigurationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationConfigurationTest.class);

  TestDataFactory testDataFactory = new TestDataFactory();

  private WikiLinksKafkaApplicationConfiguration config;

  @Before
  public void setup() {
    config = WikiLinksKafkaApplicationConfiguration.getConfiguration();
  }

  @Test
  public void retrieve_config_success() {
    LOGGER.info("High throughput configuration is: {}", config.getKafkaThroughputProducerConfiguration());
    LOGGER.info("Application configuration is: {}", config.toString());

    assertTrue(testDataFactory.getTopic().equals(config.getKafkaProducerTopic()));
    assertTrue(testDataFactory.getFirstFile().equals(config.getEventFiles().get(0)));
    assertTrue(testDataFactory.getFirstPointerFile().equals(config.getEventPointerFiles().get(0)));
  }

  @Test
  public void retrieve_config_threads_safe_success() {

    int threadPoolSize = 100;
    final ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    List<ConfigRunable> configs = getConfigRunables(threadPoolSize);
    configs.forEach(executorService::submit);
  }

  private List<ConfigRunable> getConfigRunables(int threadPoolSize) {

    List<ConfigRunable> configRunables = new ArrayList<ConfigRunable>();
    for (int i = 0; i < threadPoolSize; i++) {
      configRunables.add(new ConfigRunable());
    }
    return configRunables;
  }

  private class ConfigRunable implements Runnable {

    public void run() {
      WikiLinksKafkaApplicationConfiguration config = WikiLinksKafkaApplicationConfiguration.getConfiguration();
      String threadName = Thread.currentThread().getName();
      LOGGER.info("In thread: {}, Topic is: {}", threadName, config.getKafkaProducerTopic());

      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}
