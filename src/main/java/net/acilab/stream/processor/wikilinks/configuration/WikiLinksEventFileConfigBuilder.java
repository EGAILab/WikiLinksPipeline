package net.acilab.stream.processor.wikilinks.configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration("WikiLinksEventFileConfigBuilder")
@PropertySource("classpath:wikilinks.properties")
public class WikiLinksEventFileConfigBuilder implements EventFileConfigBuilder {

  @Value("${event.file.location}")
  private String eventFileLocation;

  @Value("${event.file.names}")
  private String eventFileNames;

  @Value("${event.pointer.file.suffix}")
  private String eventPointerFileSuffix;

  public String getEventFileLocation() {
    return eventFileLocation;
  }

  public List<String> getEventFileNames() {
    return Arrays.asList(eventFileNames.split(","));
  }

  public List<String> getEventFileList() {
    List<String> fileNameList = getEventFileNames();
    List<String> eventFileList = fileNameList.stream().map(s -> eventFileLocation + s).collect(Collectors.toList());
    return eventFileList;
  }

  public List<String> getEventPointerFileList() {
    List<String> eventFileList = getEventFileList();
    List<String> eventPointerFileList = eventFileList.stream().map(s -> s + eventPointerFileSuffix)
        .collect(Collectors.toList());
    return eventPointerFileList;
  }
}
