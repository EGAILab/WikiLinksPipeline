package net.acilab.stream.processor.wikilinks;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.TestDataFactory;
import net.acilab.stream.processor.wikilinks.configuration.WikiLinksEventFileConfigBuilder;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

@RunWith(MockitoJUnitRunner.class)
public class WikiLinksEventFileProcessorTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksEventFileProcessorTest.class);

  TestDataFactory testDataFactory = new TestDataFactory();

  @InjectMocks
  WikiLinksEventFileProcessor wikiLinksEventFileProcessor;

  @Mock
  WikiLinksEventFileConfigBuilder wikiLinksEventFileConfigBuilderMock;

  @Before
  public void setup() {

    when(wikiLinksEventFileConfigBuilderMock.getEventFileList()).thenReturn(testDataFactory.getEventFileList());
    when(wikiLinksEventFileConfigBuilderMock.getEventPointerFileList())
        .thenReturn(testDataFactory.getEventPointerFileList());
  }

  @Test
  public void verify_config_and_file_index_input() {

    wikiLinksEventFileProcessor.readEvents(0, 1);
    verify(wikiLinksEventFileConfigBuilderMock, times(1)).getEventPointerFileList();
    wikiLinksEventFileProcessor.readEvents(9, 3);
    verify(wikiLinksEventFileConfigBuilderMock, times(2)).getEventPointerFileList();

    Object ret = wikiLinksEventFileProcessor.readEvents(10, 3);
    assertNull(ret);
  }

  @Test
  public void commit_offset_success() throws Exception {

    // 592017993l
    long offset = testDataFactory.getFirstFileEndOfFileOffsets()[0];
    int ret = wikiLinksEventFileProcessor.commitOffset(offset, 0);
    assertEquals(1, ret);

    // read offfset back from offset file and verify
    long currentOffset = testDataFactory.getFirstFileCurrentOffset();
    assertTrue(currentOffset == testDataFactory.getFirstFileEndOfFileOffsets()[0]);
  }

  @Test
  public void read_two_batches_with_offset_commit_success() throws Exception {

    String eventPointerFile = testDataFactory.getFirstPointerFile();
    int BATCH_SIZE = 2;

    // delete pointer file before test
    deletePointerFile(eventPointerFile);

    // read first batch
    List<Object> retList = wikiLinksEventFileProcessor.readEvents(0, BATCH_SIZE);
    List<WikiLinksArticleEvent> eventList = (List<WikiLinksArticleEvent>) retList.get(0);
    WikiLinksArticleEvent firstEvent = eventList.get(0);
    WikiLinksArticleEvent secondEvent = eventList.get(1);
    long endOffset = (long) retList.get(1);

    long actualOffset = testDataFactory.getFirstFileOffsets()[2];
    WikiLinksArticleEvent actualFirstEvent = testDataFactory.getFirstFileEventOne();
    WikiLinksArticleEvent actualSecondEvent = testDataFactory.getFirstFileEventTwo();

    assertEquals(BATCH_SIZE, eventList.size());
    assertEquals(actualOffset, endOffset);

    assertTrue(firstEvent.getUrl().toString().equals(actualFirstEvent.getUrl().toString()));
    assertTrue(secondEvent.getUrl().toString().equals(actualSecondEvent.getUrl().toString()));

    // commit offset
    int ret = wikiLinksEventFileProcessor.commitOffset(endOffset, 0);
    assertEquals(1, ret);

    // read next batch
    retList = wikiLinksEventFileProcessor.readEvents(0, BATCH_SIZE);
    eventList = (List<WikiLinksArticleEvent>) retList.get(0);
    // WikiLinksArticleEvent thirdEvent = eventList.get(0);
    WikiLinksArticleEvent fourthEvent = eventList.get(1);
    endOffset = (long) retList.get(1);

    actualOffset = testDataFactory.getFirstFileOffsets()[4];
    WikiLinksArticleEvent actualFourthEvent = testDataFactory.getFirstFileEventFour();

    assertEquals(BATCH_SIZE, eventList.size());
    assertEquals(actualOffset, endOffset);

    assertTrue(fourthEvent.getUrl().toString().equals(actualFourthEvent.getUrl().toString()));
  }

  @Test
  public void read_boundry_end_of_file_success() throws Exception {

    String eventPointerFile = testDataFactory.getFirstPointerFile();
    long offset = testDataFactory.getFirstFileEndOfFileOffsets()[2];
    int BATCH_SIZE = 3;

    // update pointer file to point to the last 2 events
    updatePointerFile(eventPointerFile, offset);

    // read 3 events to hit the end of file
    List<Object> retList = wikiLinksEventFileProcessor.readEvents(0, BATCH_SIZE);
    List<WikiLinksArticleEvent> eventList = (List<WikiLinksArticleEvent>) retList.get(0);
    WikiLinksArticleEvent lastEvent = eventList.get(1);
    long endOffset = (long) retList.get(1);

    WikiLinksArticleEvent actualLastEvent = testDataFactory.getFirstFileLastEvent();

    assertEquals(2, eventList.size());
    assertTrue(lastEvent.getUrl().toString().equals(actualLastEvent.getUrl().toString()));

    // commit offset
    int ret = wikiLinksEventFileProcessor.commitOffset(endOffset, 0);
    assertEquals(1, ret);

    // read again, should return emtpy list as it reaches end of file
    retList = wikiLinksEventFileProcessor.readEvents(0, BATCH_SIZE);
    eventList = (List<WikiLinksArticleEvent>) retList.get(0);
    assertTrue(eventList.isEmpty());
  }

  // utility method
  private void deletePointerFile(String eventPointerFile) {
    try {
      File file = new File(eventPointerFile);
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      LOGGER.error("Unable to delete pointer file.");
      e.printStackTrace();
    }
  }

  // utility method
  private void updatePointerFile(String eventPointerFile, long offset) {
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(eventPointerFile, false));
      writer.write(Long.toString(offset));
      writer.close();
    } catch (IOException e) {
      LOGGER.error("Unable to update pointer file.");
      e.printStackTrace();
    }
  }
}
