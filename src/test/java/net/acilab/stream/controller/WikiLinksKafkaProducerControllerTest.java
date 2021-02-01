package net.acilab.stream.controller;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import net.acilab.stream.TestDataFactory;
import net.acilab.stream.processor.kafka.WikiLinksKafkaProducerRunable;
import net.acilab.stream.processor.kafka.configuration.KafkaProducerThroughputConfigBuilder;

@Ignore
@RunWith(MockitoJUnitRunner.class)
public class WikiLinksKafkaProducerControllerTest {

  TestDataFactory testDataFactory = new TestDataFactory();

  @InjectMocks
  private WikiLinksKafkaProducerController wikiLinksKafkaProducerController;

  @Mock
  KafkaProducerThroughputConfigBuilder kafkaProducerThroughputConfigBuilderMock;

  @Mock
  WikiLinksKafkaProducerRunable wikiLinksKafkaProducerRunableMock;

  @Test
  public void initial_producer_once_mock_success() {

    when(kafkaProducerThroughputConfigBuilderMock.getProducerConfiguration())
        .thenReturn(testDataFactory.getThroughputProducerProperties());
    when(kafkaProducerThroughputConfigBuilderMock.getTopic()).thenReturn(testDataFactory.getTopic());

    wikiLinksKafkaProducerController.initializeKafkaProducer();

    verify(kafkaProducerThroughputConfigBuilderMock, times(1)).getProducerConfiguration();
    verify(wikiLinksKafkaProducerRunableMock, times(1)).run();
  }
}
