package net.acilab.stream.processor.kafka;

import org.apache.kafka.clients.producer.Producer;

import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

public interface ProducerRunable {

  public void prepareProducer(Producer<String, WikiLinksArticleEvent> producer, String topic, int fileIndex,
      int batchSize, boolean runOnce);

  void run();
}
