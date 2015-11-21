package cgss.dev.pipeline;

import cgss.dev.model.EventHandler;
import cgss.dev.storage.KVStorage;
import cgss.dev.storage.KVStorageException;
import cgss.dev.storage.KeyValue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

/**
 * Thread that runs the Kafka to KVStorage pipeline.
 */
public class KafkaSinkThread extends Thread {
    /**
     * Default logger for this class.
     */
    private final static Logger logger = Logger.getLogger(KafkaSinkThread.class.getSimpleName());
    /**
     * Kafka consumer to consumer the events from.
     */
    private final Consumer<byte[], byte[]> kafkaConsumer;
    /**
     * Pool interval for consuming events.
     */
    private final Duration pollIntervalDuration;
    /**
     * The key value storage to store the Kafka events in.
     */
    private final KVStorage kvStorage;
    /**
     * The event business object.
     */
    private final EventHandler eventHandler;
    /**
     * The number of consumed messages. This is used as a key for the KVStorage.
     */
    private Long numConsumedMessages;

    /**
     * The constructor for the Kafka sink pipeline thread.
     *
     * @param kafkaConsumer The Kafka consumer to read messages from.
     * @param pollIntervalDuration How often to poll the consumer for messages.
     * @param kvStorage The storage to save the events into.
     * @param eventHandler The event business object.
     */
    public KafkaSinkThread(
            final Consumer<byte[], byte[]> kafkaConsumer,
            final Duration pollIntervalDuration,
            final KVStorage kvStorage,
            final EventHandler eventHandler
    ) {
        this.kafkaConsumer = kafkaConsumer;
        this.pollIntervalDuration = pollIntervalDuration;
        this.kvStorage = kvStorage;
        this.eventHandler = eventHandler;
        this.numConsumedMessages = Long.MIN_VALUE;
    }

    /**
     * Stops the pipeline and closes the Kafka consumer.
     */
    @Override
    public void interrupt() {
        super.interrupt();

        kafkaConsumer.close();
    }

    /**
     * Runs the consumer thread.
     */
    @Override
    public void run() {
        while (!this.isInterrupted()){
            final ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(pollIntervalDuration.toMillis());

            logger.info(String.format("Got %d records from Kafka.", consumerRecords.count()));

            final boolean savingFailed = StreamSupport
                    .stream(consumerRecords.spliterator(), false)
                    .filter(this::validateRecord)
                    .map(this::trySaveToStorage)
                    .anyMatch(succeeded -> !succeeded);

            if (!savingFailed) {
                kafkaConsumer.commitSync();
            } else {
                logger.warning("Failed saving to storage.");
            }
        }
    }

    /**
     * Validates if the given record is a valid event.
     *
     * @param record The record to be checked.
     * @return If the given record is valid or not.
     */
    private boolean validateRecord(final ConsumerRecord<byte[], byte[]> record) {
        final boolean isValid = eventHandler.validateEvent(record.value());

        if (!isValid){
            logger.warning(String.format("Invalid event received: %s", record));
        }

        return isValid;
    }

    /**
     * Tries to save the given record to the key value storage.
     *
     * @param record The record to be saved.
     * @return If it successfully saved the record.
     */
    private boolean trySaveToStorage(final ConsumerRecord<byte[], byte[]> record) {
        final byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(numConsumedMessages++).array();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try {
            final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            objectOutputStream.writeObject(new KeyValue(record.key(), record.value()));
            objectOutputStream.close();

            kvStorage.SaveValue(new KeyValue(key, byteArrayOutputStream.toByteArray()));

            logger.info(String.format("Saved %s", record));
        } catch (final KVStorageException | IOException e) {
            numConsumedMessages--;

            logger.warning(String.format("Failed saving %s", record));

            return false;
        }

        return true;
    }
}
