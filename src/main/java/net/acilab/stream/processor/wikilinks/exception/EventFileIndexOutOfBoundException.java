package net.acilab.stream.processor.wikilinks.exception;

public class EventFileIndexOutOfBoundException extends Exception {

  public EventFileIndexOutOfBoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public EventFileIndexOutOfBoundException(String message) {
    super(message);
  }

  public EventFileIndexOutOfBoundException(Throwable cause) {
    super(cause);
  }
}
