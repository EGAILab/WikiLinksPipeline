package net.acilab.stream.processor.kafka;

import java.util.List;
import java.util.concurrent.Future;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

import net.acilab.stream.processor.wikilinks.EventFileProcessor;
import net.acilab.stream.processor.wikilinks.WikiLinksEventFileProcessor;

import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class WikiLinksKafkaProducerRunable implements ProducerRunable, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaProducerRunable.class);

  EventFileProcessor eventFileProcessor;
  private Producer<String, WikiLinksArticleEvent> producer;

  // for test purpose
  private static final boolean RUN_ONCE = true;

  private String topic;
  private int fileIndex;
  private int batchSize;

  @Inject
  void setEventFileProcessor(WikiLinksEventFileProcessor eventFileProcessor) {
    this.eventFileProcessor = eventFileProcessor;
  }

  public void prepareProducer(Producer<String, WikiLinksArticleEvent> producer, String topic, int fileIndex,
      int batchSize) {
    this.producer = producer;
    this.topic = topic;
    this.fileIndex = fileIndex;
    this.batchSize = batchSize;
  }

  @Override
  public void run() {
    LOGGER.info("=== Starting WikiLinksKafkaProducerRunable ===");
    LOGGER.info("Producer Topic is: {}", topic);

    try {
      while (true) {
        // Read from data file
        List<Object> retList = eventFileProcessor.readEvents(fileIndex, batchSize);
        List<WikiLinksArticleEvent> eventList = (List<WikiLinksArticleEvent>) retList.get(0);
        long endOffset = (long) retList.get(1);

        // if empty list, end of file
        if (eventList.isEmpty()) {
          LOGGER.info("End of file reached.");
          break;
        }

        // Send to Kafka
        WikiLinksArticleEvent event;
        String key;
        for (int i = 0; i < eventList.size(); i++) {
          event = eventList.get(i);
          // LOGGER.info("Event is: {}", event.toString());
          key = event.getUrl().toString();
          ProducerRecord<String, WikiLinksArticleEvent> producerRecord = new ProducerRecord<>(this.topic, key, event);

          // final Future<RecordMetadata> future = producer.send(producerRecord);
          // producer.flush();
          producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              if (e != null) {
                LOGGER.error("Callback::Error while producing message to topic: {}", recordMetadata.topic());
                e.printStackTrace();
              } else {
                LOGGER.info("Sent message to topic:{} partition:{} offset:{}", recordMetadata.topic(),
                    recordMetadata.partition(), recordMetadata.offset());
              }
            }
          });
          LOGGER.info("Successfully sent out all messages in the batch to topic {}", this.topic);
        }

        // Commit offset
        int ret = eventFileProcessor.commitOffset(endOffset, fileIndex);
        if (ret == 0) {
          LOGGER.error("Error committing offset to pointer file.");
          break;
        }

        if (RUN_ONCE) {
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error while starting producer, error is: {}", e.toString());
      e.printStackTrace();
    } finally {
      // Clean up
      if (producer != null)
        producer.close();
      // LOGGER.info("Producer closed.");
    }
  }
}
