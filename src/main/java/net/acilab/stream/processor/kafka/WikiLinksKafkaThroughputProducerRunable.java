package net.acilab.stream.processor.kafka;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.inject.Inject;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.acilab.stream.processor.wikilinks.EventFileProcessor;
import net.acilab.stream.processor.wikilinks.serialization.WikiLinksArticleEvent;

public class WikiLinksKafkaThroughputProducerRunable implements Runnable, WikiLinksKafkaProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiLinksKafkaThroughputProducerRunable.class);

  private final Producer<String, WikiLinksArticleEvent> producer;
  private final EventFileProcessor eventFileProcessor;
  private final String topic;
  private final int fileIndex;
  private final int batchSize;
  private final boolean runOnce;

  @Inject
  public WikiLinksKafkaThroughputProducerRunable(Producer<String, WikiLinksArticleEvent> producer,
      EventFileProcessor eventFileProcessor, String topic, int fileIndex, int batchSize, boolean runOnce) {
    this.producer = producer;
    this.eventFileProcessor = eventFileProcessor;
    this.topic = topic;
    this.fileIndex = fileIndex;
    this.batchSize = batchSize;
    this.runOnce = runOnce;
  }

  @Override
  public void run() {

    LOGGER.info("=== Starting WikiLinksKafkaThroughputProducer ===");
    LOGGER.info("Producer Topic is: {}", topic);
    LOGGER.info("File index is: {}", fileIndex);
    LOGGER.info("Batch size is: {}", batchSize);

    int sentCount = 0;

    while (true) {
      // Read from data file
      final List<Object> retList = eventFileProcessor.readEvents(fileIndex, batchSize);
      final List<WikiLinksArticleEvent> eventList = (List<WikiLinksArticleEvent>) retList.get(0);
      final long endOffset = (long) retList.get(1);

      // if empty list, end of file
      if (eventList.isEmpty()) {
        LOGGER.info("End of file reached.");
        break;
      }

      // Send to Kafka
      for (int i = 0; i < eventList.size(); i++) {
        sentCount++;
        final WikiLinksArticleEvent event = eventList.get(i);
        final String key = event.getUrl().toString();
        final ProducerRecord<String, WikiLinksArticleEvent> producerRecord = new ProducerRecord<>(this.topic, key,
            event);

        try {
          final Future<RecordMetadata> future = producer.send(producerRecord);

          // display RecordMetaData every 100 events
          if (sentCount % 100 == 0) {
            displayRecordMetaData(producerRecord, future);
          }
        } catch (InterruptedException ie) {
          if (Thread.interrupted()) {
            break;
          }
        } catch (ExecutionException ee) {
          LOGGER.error("Error when sending event to topic {}. Error is: {}.", topic, ee);
        }
      }

      // Commit offset (might need to move to outside the loop, less I/O)
      final int ret = eventFileProcessor.commitOffset(endOffset, fileIndex);
      if (ret == 0) {
        LOGGER.error("Error committing offset to pointer file.");
        break;
      } else {
        // LOGGER.info("Successfully commit file offset, offset is: {}", endOffset);
      }

      if (runOnce) {
        break;
      }
    }
  }

  private void displayRecordMetaData(final ProducerRecord<String, WikiLinksArticleEvent> event,
      final Future<RecordMetadata> future) throws InterruptedException, ExecutionException {

    final RecordMetadata recordMetadata = future.get();

    LOGGER.info(String.format("Event successfully sent to topic=%s part=%d off=%d at time=%s", recordMetadata.topic(),
        recordMetadata.partition(), recordMetadata.offset(), new Date(recordMetadata.timestamp())));
  }
}
