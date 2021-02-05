package net.acilab.stream.processor.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
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

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.processor.wikilinks.EventFileProcessor;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;
import net.acilab.stream.TestDataFactory;

// @Ignore
@RunWith(MockitoJUnitRunner.class)
public class WikiLinksKafkaThroughputProducerRunableTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaThroughputProducerRunableTest.class);

  TestDataFactory testDataFactory = new TestDataFactory();

  // @InjectMocks
  private WikiLinksKafkaThroughputProducerRunable wikiLinksKafkaThroughputProducer;

  @Mock
  private EventFileProcessor eventFileProcessorMock;

  private MockProducer<String, WikiLinksArticleEvent> producerMock;
  private String topic;
  private int fileIndex;
  private int batchSize;
  private boolean runOnce;

  @Before
  public void setup() {
    producerMock = new MockProducer<>(true, null, null);
    topic = testDataFactory.getTopic();
    fileIndex = 0;
    batchSize = 3;
    runOnce = true;

    // mocking readEvents()
    List<WikiLinksArticleEvent> eventList = new ArrayList<WikiLinksArticleEvent>();
    eventList.add(testDataFactory.getFirstFileEventOne());
    eventList.add(testDataFactory.getFirstFileEventTwo());
    eventList.add(testDataFactory.getFirstFileEventThree());
    long endOffset = testDataFactory.getFirstFileOffsets()[3];
    List<Object> objList = Arrays.asList(eventList, endOffset);

    when(eventFileProcessorMock.readEvents(anyInt(), anyInt())).thenReturn(objList);
    when(eventFileProcessorMock.commitOffset(anyLong(), anyInt())).thenReturn(1);
  }

  @Test
  public void produce_events_success() {
    wikiLinksKafkaThroughputProducer = new WikiLinksKafkaThroughputProducerRunable(producerMock, eventFileProcessorMock,
        topic, fileIndex, batchSize, runOnce);
    wikiLinksKafkaThroughputProducer.run();

    long endOffset = testDataFactory.getFirstFileOffsets()[3];

    verify(eventFileProcessorMock, times(1)).readEvents(fileIndex, batchSize);
    verify(eventFileProcessorMock, times(1)).commitOffset(endOffset, fileIndex);

    assertTrue(producerMock.history().size() == 3);
  }
}
