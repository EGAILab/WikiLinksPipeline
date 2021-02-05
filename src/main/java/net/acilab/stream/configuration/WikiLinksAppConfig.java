package net.acilab.stream.configuration;

import java.util.List;

public interface WikiLinksAppConfig {

  public String getEventFileLocation();

  public List<String> getEventFileNames();

  public List<String> getEventFiles();

  public List<String> getEventPointerFiles();

  public int getEventFileTotal();

  public int getEventFileReadBatchSize();
}
