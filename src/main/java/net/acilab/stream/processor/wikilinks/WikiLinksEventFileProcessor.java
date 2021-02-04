package net.acilab.stream.processor.wikilinks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;

import org.springframework.stereotype.Repository;

import net.acilab.stream.processor.wikilinks.configuration.EventFileConfigBuilder;
import net.acilab.stream.processor.wikilinks.configuration.WikiLinksEventFileConfigBuilder;
import net.acilab.stream.processor.wikilinks.exception.EventFileIndexOutOfBoundException;
import net.acilab.stream.utils.ApplicationConstants;

import net.acilab.stream.processor.wikilinks.serialization.Mention;
import net.acilab.stream.processor.wikilinks.serialization.Token;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Repository
public class WikiLinksEventFileProcessor implements EventFileProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksEventFileProcessor.class);

  private EventFileConfigBuilder fileConfigBuilder;

  private String eventFile;
  private String eventPointerFile;
  private BufferedReader bufferReader;
  private BufferedWriter bufferWriter;
  private RandomAccessFile eventFileStream;

  @Inject
  public WikiLinksEventFileProcessor(EventFileConfigBuilder fileConfigBuilder) {
    this.fileConfigBuilder = fileConfigBuilder;
  }

  private long readOffset() throws Exception {
    if (!(new File(eventPointerFile).exists())) {
      // file doesn't exist, return position 0
      return 0;
    } else {
      bufferReader = new BufferedReader(new FileReader(eventPointerFile));
      String offset = bufferReader.readLine();
      bufferReader.close();
      return Long.parseLong(offset);
    }
  }

  @Override
  public List<Object> readEvents(int fileIndex, int batchSize) {
    LOGGER.info("=== Starting WikiLinksEventFileProcessor ===");

    List<Object> mentions = new ArrayList<Object>();
    List<Object> tokens = new ArrayList<Object>();
    String url = "";
    List<WikiLinksArticleEvent> wikiLinksEventList = new ArrayList<WikiLinksArticleEvent>();

    try {
      if (!ApplicationConstants.EVENT_FILE_INDEX_RANGE.contains(fileIndex)) {
        throw new EventFileIndexOutOfBoundException("Invalid file index: " + fileIndex);
      }

      eventFile = fileConfigBuilder.getEventFileList().get(fileIndex);
      eventPointerFile = fileConfigBuilder.getEventPointerFileList().get(fileIndex);
      LOGGER.info("Event file is: {}", eventFile);
      LOGGER.info("Event pointer file is: {}", eventPointerFile);

      long initOffset = readOffset();
      LOGGER.info("Read starting from offset: {}", initOffset);
      eventFileStream = new RandomAccessFile(eventFile, "r");
      eventFileStream.seek(initOffset);

      int newline_count = 0;
      String line = "";
      long endOffset = initOffset;

      for (int i = 0; i < batchSize; i++) {
        while (true) {
          if ((line = eventFileStream.readLine()) == null) {
            // end of the file, return empty list, caller needs to handle this
            endOffset = eventFileStream.getFilePointer();
            return Arrays.asList(wikiLinksEventList, endOffset);
          } else {
            // event: [URL][MENTION...][TOKEN...]
            if (line.startsWith("URL")) {
              url = line.split("\t")[1];
              newline_count = 0;
            }
            if (line.startsWith("MENTION")) {
              Mention mention = Mention.newBuilder().setMentionstring(line.split("\t")[1])
                  .setByteoffset(Long.parseLong(line.split("\t")[2])).setTargeturl(line.split("\t")[3]).build();
              mentions.add(mention);
              newline_count = 0;
            }
            if (line.startsWith("TOKEN")) {
              Token token = Token.newBuilder().setTokenstring(line.split("\t")[1])
                  .setByteoffset(Long.parseLong(line.split("\t")[2])).build();
              tokens.add(token);
              newline_count = 0;
            }

            newline_count++;
            // end of event
            if (newline_count == 3) {
              WikiLinksArticleEvent wikiLinksEvent = WikiLinksArticleEvent.newBuilder().setUrl(url)
                  .setMentions(mentions).setTokens(tokens).build();
              wikiLinksEventList.add(wikiLinksEvent);
              break;
            }
          }
        }
      }
      endOffset = eventFileStream.getFilePointer();
      return Arrays.asList(wikiLinksEventList, endOffset);
    } catch (IOException ioe) {
      LOGGER.error("IO error in reading file.");
      ioe.printStackTrace();
      return null;
    } catch (Exception e) {
      LOGGER.error("Other error in reading file, error is: {}", e.toString());
      e.printStackTrace();
      return null;
    } finally {
      try {
        if (bufferReader != null)
          bufferReader.close();
        if (bufferWriter != null)
          bufferWriter.close();
        if (eventFileStream != null)
          eventFileStream.close();
      } catch (Exception ex) {
        LOGGER.error("Error in closing file, error is: {}", ex.toString());
        ex.printStackTrace();
      }
    }
  }

  // 1 - successful
  // 0 - failed
  public int commitOffset(long offset, int fileIndex) {
    BufferedWriter commitOffsetBufferWriter;
    String eventPointerFileToCommit = fileConfigBuilder.getEventPointerFileList().get(fileIndex);
    try {
      commitOffsetBufferWriter = new BufferedWriter(new FileWriter(eventPointerFileToCommit, false));
      commitOffsetBufferWriter.write(Long.toString(offset));
      commitOffsetBufferWriter.close();
      return 1;
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error("Error in committing offset, error is: {}", e.toString());
      return 0;
    }
  }
}
