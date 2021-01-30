package net.acilab.stream.processor.kafka;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import net.acilab.stream.processor.wikilinks.WikiLinksEventFileProcessor;
import net.acilab.stream.TestDataFactory;

@RunWith(MockitoJUnitRunner.class)
public class WikiLinksKafkaProducerRunableTest {

  TestDataFactory testDataFactory = new TestDataFactory();

  @InjectMocks
  private WikiLinksKafkaProducerRunable wikiLinksKafkaProducerRunable;

  @Mock
  private WikiLinksEventFileProcessor wikiLinksEventFileProcessorMock;

  @Test
  public void run_producer_once_mock_success() {

    Properties producerConfig = testDataFactory.getThroughputProducerProperties();
    KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerConfig);

    wikiLinksKafkaProducerRunable.prepareProducer(producer, testDataFactory.getTopic(), 0);
    wikiLinksKafkaProducerRunable.run();

    verify(wikiLinksEventFileProcessorMock, times(1)).readNextEvent(0);
  }
}
