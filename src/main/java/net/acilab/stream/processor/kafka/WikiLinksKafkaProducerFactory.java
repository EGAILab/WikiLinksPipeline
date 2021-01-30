package net.acilab.stream.processor.kafka;

public class WikiLinksKafkaProducerFactory implements ProducerFactory {
  public static ProducerRunable getProducerRunable(int fileIndex) {
    return new WikiLinksKafkaProducerRunable();
  }
}
