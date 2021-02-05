package net.acilab.stream.processor.wikilinks.configuration;

import java.util.List;

public interface EventFileConfigBuilder {

  public String getEventFileLocation();

  public List<String> getEventFileNames();

  public List<String> getEventFiles();

  public List<String> getEventPointerFiles();

  public int getEventFileTotal();

  public int getEventFileReadBatchSize();
}
