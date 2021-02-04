package net.acilab.stream.processor.wikilinks.configuration;

import java.util.List;

public interface EventFileConfigBuilder {

  public String getEventFileLocation();

  public List<String> getEventFileNames();

  public List<String> getEventFileList();

  public List<String> getEventPointerFileList();

  public int getEventFileNumber();

  public int getBatchSize();
}
