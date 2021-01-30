package net.acilab.stream.processor.wikilinks;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Repository;

import net.acilab.stream.processor.wikilinks.configuration.WikiLinksEventFileConfigBuilder;
import net.acilab.stream.processor.wikilinks.exception.EventFileIndexOutOfBoundException;
import net.acilab.stream.utils.ApplicationConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Repository
public class WikiLinksEventFileProcessor implements EventFileProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksEventFileProcessor.class);

  private WikiLinksEventFileConfigBuilder fileConfigBuilder;

  private List<String> eventFileList;
  private List<String> eventPointerFileList;

  @Inject
  public WikiLinksEventFileProcessor(WikiLinksEventFileConfigBuilder fileConfigBuilder) {
    this.fileConfigBuilder = fileConfigBuilder;
  }

  @Override
  public List<Object> readNextEvent(int fileIndex) {
    LOGGER.info("=== Starting WikiLinksEventFileProcessor ===");

    try {
      if (!ApplicationConstants.EVENT_FILE_INDEX_RANGE.contains(fileIndex)) {
        throw new EventFileIndexOutOfBoundException("Invalid file index: " + fileIndex);
      }

      this.eventFileList = fileConfigBuilder.getEventFileList();
      this.eventPointerFileList = fileConfigBuilder.getEventPointerFileList();

      LOGGER.info("Event file is: {}", this.eventFileList.get(fileIndex).toString());
      LOGGER.info("Event pointer file is: {}", this.eventPointerFileList.get(fileIndex).toString());

      return null;
    } catch (Exception e) {
      LOGGER.error("Other error in reading file, error is: {}", e.toString());
      e.printStackTrace();
      return null;
    }
  }
}
