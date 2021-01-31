package net.acilab.stream.processor.wikilinks;

import static org.junit.Assert.assertTrue;
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
  public void read_event_verify_file_index() {

    wikiLinksEventFileProcessor.readNextEvent(0);
    verify(wikiLinksEventFileConfigBuilderMock, times(1)).getEventPointerFileList();
    wikiLinksEventFileProcessor.readNextEvent(9);
    verify(wikiLinksEventFileConfigBuilderMock, times(2)).getEventPointerFileList();

    Object ret = wikiLinksEventFileProcessor.readNextEvent(10);
    assertNull(ret);
  }

  @Test
  public void read_first_event_success_test() throws Exception {

    String eventPointerFile = testDataFactory.getFirstPointerFile();

    // delete pointer file before test
    deletePointerFile(eventPointerFile);

    List<Object> retList = wikiLinksEventFileProcessor.readNextEvent(0);
    WikiLinksArticleEvent event = (WikiLinksArticleEvent) retList.get(0);

    WikiLinksArticleEvent actualEvent = testDataFactory.getFirstFileFirstEvent();
    assertTrue(event.getUrl().toString().equals(actualEvent.getUrl().toString()));
  }

  @Test
  public void read_second_event_success_test() throws Exception {

    String eventPointerFile = testDataFactory.getFirstPointerFile();
    String offset = testDataFactory.getFirstFileSecondEventOffset();

    // update pointer file to point to the second event
    updatePointerFile(eventPointerFile, offset);

    List<Object> retList = wikiLinksEventFileProcessor.readNextEvent(0);
    WikiLinksArticleEvent event = (WikiLinksArticleEvent) retList.get(0);

    WikiLinksArticleEvent actualEvent = testDataFactory.getFirstFileSecondEvent();
    assertTrue(event.getUrl().toString().equals(actualEvent.getUrl().toString()));
  }

  @Test
  public void read_end_of_file_success_test() throws Exception {

    String eventPointerFile = testDataFactory.getFirstPointerFile();
    String offset = testDataFactory.getFirstFileEndOfFileOffset();

    // update pointer file to point to the end of file
    updatePointerFile(eventPointerFile, offset);

    List<Object> retList = wikiLinksEventFileProcessor.readNextEvent(0);
    WikiLinksArticleEvent event = (WikiLinksArticleEvent) retList.get(0);

    assertTrue(event.getUrl().equals(""));
  }

  @Test
  public void rollback_offset_success_test() throws Exception {

    String eventPointerFile = testDataFactory.getFirstPointerFile();

    // delete pointer file before test
    deletePointerFile(eventPointerFile);

    // read from beginning
    List<Object> retList = wikiLinksEventFileProcessor.readNextEvent(0);
    // offset returned (before read) is 0
    String offset = retList.get(1).toString();

    // rollback offset
    int ret = wikiLinksEventFileProcessor.rollbackOffset(offset);

    // read the offset file again, the offset after rollback should be 0
    String currentOffset = testDataFactory.getFirstFileCurrentOffset();

    assertTrue(ret == 1 && currentOffset.equals("0"));
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
  private void updatePointerFile(String eventPointerFile, String offset) {
    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(eventPointerFile, false));
      writer.write(offset);
      writer.close();
    } catch (IOException e) {
      LOGGER.error("Unable to update pointer file.");
      e.printStackTrace();
    }
  }
}
