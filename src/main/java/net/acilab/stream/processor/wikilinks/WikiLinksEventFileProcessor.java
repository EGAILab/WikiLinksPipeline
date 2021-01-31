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

  private WikiLinksEventFileConfigBuilder fileConfigBuilder;

  private String eventFile;
  private String eventPointerFile;
  private BufferedReader bufferReader;
  private BufferedWriter bufferWriter;
  private RandomAccessFile eventFileStream;
  private WikiLinksArticleEvent wikiLinksEvent;

  @Inject
  public WikiLinksEventFileProcessor(WikiLinksEventFileConfigBuilder fileConfigBuilder) {
    this.fileConfigBuilder = fileConfigBuilder;
  }

  private String readOffset() throws Exception {
    if (!(new File(eventPointerFile).exists())) {
      // file doesn't exist, return position 0
      return "0";
    } else {
      bufferReader = new BufferedReader(new FileReader(eventPointerFile));
      String offset = bufferReader.readLine();
      bufferReader.close();
      return offset;
    }
  }

  private void writeOffset(String offset) throws Exception {
    bufferWriter = new BufferedWriter(new FileWriter(eventPointerFile, false));
    bufferWriter.write(offset);
    bufferWriter.close();
  }

  @Override
  public List<Object> readNextEvent(int fileIndex) {
    LOGGER.info("=== Starting WikiLinksEventFileProcessor ===");

    List<Object> mentions = new ArrayList<Object>();
    List<Object> tokens = new ArrayList<Object>();
    String url = "";
    // empty event
    wikiLinksEvent = WikiLinksArticleEvent.newBuilder().setUrl(url).setMentions(mentions).setTokens(tokens).build();

    try {
      if (!ApplicationConstants.EVENT_FILE_INDEX_RANGE.contains(fileIndex)) {
        throw new EventFileIndexOutOfBoundException("Invalid file index: " + fileIndex);
      }

      eventFile = fileConfigBuilder.getEventFileList().get(fileIndex);
      eventPointerFile = fileConfigBuilder.getEventPointerFileList().get(fileIndex);
      LOGGER.info("Event file is: {}", eventFile);
      LOGGER.info("Event pointer file is: {}", eventPointerFile);

      long offset = Long.parseLong(readOffset());
      LOGGER.info("Offset is: {}", offset);

      eventFileStream = new RandomAccessFile(eventFile, "r");
      eventFileStream.seek(offset);

      int newline_count = 0;
      String line = "";
      while (true) {
        if ((line = eventFileStream.readLine()) == null) {
          // end of the file, return empty event, caller needs to handle this
          eventFileStream.close();
          return Arrays.asList(wikiLinksEvent, offset);
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
            wikiLinksEvent = WikiLinksArticleEvent.newBuilder().setUrl(url).setMentions(mentions).setTokens(tokens)
                .build();
            long new_offset = eventFileStream.getFilePointer();
            writeOffset(Long.toString(new_offset));
            return Arrays.asList(wikiLinksEvent, offset);
          }
        }
      }
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
  public int rollbackOffset(String offset) {
    BufferedWriter rollbackBufferWriter;
    try {
      rollbackBufferWriter = new BufferedWriter(new FileWriter(eventPointerFile, false));
      rollbackBufferWriter.write(offset);
      rollbackBufferWriter.close();
      return 1;
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error("Error in rolling back offset, error is: {}", e.toString());
      return 0;
    }
  }
}
