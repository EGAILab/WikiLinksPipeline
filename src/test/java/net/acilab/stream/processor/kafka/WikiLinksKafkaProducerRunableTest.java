package net.acilab.stream.processor.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.processor.wikilinks.WikiLinksEventFileProcessor;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;
import net.acilab.stream.TestDataFactory;

@RunWith(MockitoJUnitRunner.class)
public class WikiLinksKafkaProducerRunableTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaProducerRunableTest.class);

  TestDataFactory testDataFactory = new TestDataFactory();

  @InjectMocks
  private WikiLinksKafkaProducerRunable wikiLinksKafkaProducerRunable;

  @Mock
  private WikiLinksEventFileProcessor wikiLinksEventFileProcessorMock;

  @Before
  public void setup() {

    // mocking readEvents()
    List<WikiLinksArticleEvent> eventList = new ArrayList<WikiLinksArticleEvent>();
    eventList.add(testDataFactory.getFirstFileEventOne());
    eventList.add(testDataFactory.getFirstFileEventTwo());
    eventList.add(testDataFactory.getFirstFileEventThree());
    long endOffset = testDataFactory.getFirstFileOffsets()[3];
    List<Object> objList = Arrays.asList(eventList, endOffset);

    when(wikiLinksEventFileProcessorMock.readEvents(anyInt(), anyInt())).thenReturn(objList);
    when(wikiLinksEventFileProcessorMock.commitOffset(anyLong(), anyInt())).thenReturn(1);
  }

  @Test
  public void produce_events_mock_success() {

    // Properties producerConfig =
    // testDataFactory.getThroughputProducerProperties();
    // KafkaProducer<String, WikiLinksArticleEvent> producer = new
    // KafkaProducer<>(producerConfig);
    MockProducer<String, WikiLinksArticleEvent> producerMock = new MockProducer<>(true, null, null);
    String topic = testDataFactory.getTopic();
    int fileIndex = 0;
    int batchSize = 3;

    long endOffset = testDataFactory.getFirstFileOffsets()[3];

    wikiLinksKafkaProducerRunable.prepareProducer(producerMock, topic, fileIndex, batchSize);
    wikiLinksKafkaProducerRunable.run();

    verify(wikiLinksEventFileProcessorMock, times(1)).readEvents(fileIndex, batchSize);
    verify(wikiLinksEventFileProcessorMock, times(1)).commitOffset(endOffset, fileIndex);

    // LOGGER.info("Mock history is: {}", producerMock.history().toString());
    assertTrue(producerMock.history().size() == 3);
  }

  @Ignore
  @Test
  public void exception_test() {

    MockProducer<String, WikiLinksArticleEvent> producerMock = new MockProducer<>(true, null, null);
    String topic = testDataFactory.getTopic();
    int fileIndex = 0;
    int batchSize = 3;

    wikiLinksKafkaProducerRunable.prepareProducer(producerMock, topic, fileIndex, batchSize);
    wikiLinksKafkaProducerRunable.run();

    producerMock.errorNext(new RuntimeException());
    // ???
  }
}
