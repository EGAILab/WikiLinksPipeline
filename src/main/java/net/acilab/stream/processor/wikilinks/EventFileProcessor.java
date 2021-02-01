package net.acilab.stream.processor.wikilinks;

import java.util.List;

public interface EventFileProcessor {
  public List<Object> readEvents(int fileIndex, int batchSize);
  public int commitOffset(long offset, int fileIndex);
}
