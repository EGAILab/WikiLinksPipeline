package net.acilab.stream.processor.wikilinks.configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import net.acilab.stream.configuration.WikiLinksAppConfig;

@Configuration("WikiLinksEventFileConfigBuilder")
@PropertySource("classpath:wikilinks.properties")
public class WikiLinksEventFileConfigBuilder implements EventFileConfigBuilder, WikiLinksAppConfig {

  @Value("${event.file.location}")
  private String eventFileLocation;

  @Value("${event.pointer.file.suffix}")
  private String eventPointerFileSuffix;

  @Value("${event.file.names}")
  private String eventFileNames;

  @Value("${event.file.number}")
  private String eventFileNumber;

  @Value("${event.file.batch.size}")
  private String eventBatchSize;

  public String getEventFileLocation() {
    return eventFileLocation;
  }

  public List<String> getEventFileNames() {
    return Arrays.asList(eventFileNames.split(","));
  }

  public List<String> getEventFiles() {
    List<String> fileNameList = getEventFileNames();
    List<String> eventFileList = fileNameList.stream().map(s -> eventFileLocation + s).collect(Collectors.toList());
    return eventFileList;
  }

  public List<String> getEventPointerFiles() {
    List<String> eventFileList = getEventFiles();
    List<String> eventPointerFileList = eventFileList.stream().map(s -> s + eventPointerFileSuffix)
        .collect(Collectors.toList());
    return eventPointerFileList;
  }

  public int getEventFileTotal() {
    return Integer.parseInt(eventFileNumber);
  }

  public int getEventFileReadBatchSize() {
    return Integer.parseInt(eventBatchSize);
  }
}
