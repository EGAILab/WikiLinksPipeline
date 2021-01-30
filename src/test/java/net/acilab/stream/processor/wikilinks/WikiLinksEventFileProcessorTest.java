package net.acilab.stream.processor.wikilinks;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import net.acilab.stream.TestDataFactory;
import net.acilab.stream.processor.wikilinks.configuration.WikiLinksEventFileConfigBuilder;

@RunWith(MockitoJUnitRunner.class)
public class WikiLinksEventFileProcessorTest {

  TestDataFactory testDataFactory = new TestDataFactory();

  @InjectMocks
  WikiLinksEventFileProcessor wikiLinksEventFileProcessor;

  @Mock
  WikiLinksEventFileConfigBuilder wikiLinksEventFileConfigBuilderMock;

  @Test
  public void read_next_event_mock_success() {

    when(wikiLinksEventFileConfigBuilderMock.getEventFileList()).thenReturn(testDataFactory.getEventFileList());
    when(wikiLinksEventFileConfigBuilderMock.getEventPointerFileList())
        .thenReturn(testDataFactory.getEventPointerFileList());

    wikiLinksEventFileProcessor.readNextEvent(0);
    verify(wikiLinksEventFileConfigBuilderMock, times(1)).getEventPointerFileList();
    wikiLinksEventFileProcessor.readNextEvent(9);
    verify(wikiLinksEventFileConfigBuilderMock, times(2)).getEventPointerFileList();
  }

  @Test
  public void read_next_event_mock_outofbound_fail() {
    Object ret = wikiLinksEventFileProcessor.readNextEvent(10);
    assertNull(ret);
  }
}
