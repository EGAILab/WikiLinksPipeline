package net.acilab.stream.processor.wikilinks;

public interface EventFileProcessor {
  public Object readNextEvent(int fileIndex);
}
