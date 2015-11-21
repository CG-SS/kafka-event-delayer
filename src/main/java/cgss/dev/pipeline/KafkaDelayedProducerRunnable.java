package cgss.dev.pipeline;

import cgss.dev.model.Event;
import cgss.dev.model.EventHandler;
import cgss.dev.storage.KVStorage;
import cgss.dev.storage.KVStorageException;
import cgss.dev.storage.KeyValue;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.javatuples.Pair;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A runnable responsible for running the storage to Kafka pipeline.
 * This pipeline is supposed to be run every so often, and that's why it's a runnable.
 * It will check the given storage for expired events and send sink them into Kafka.
 */
public class KafkaDelayedProducerRunnable implements Runnable {
    /**
     * The default logger for this class.
     */
    private final static Logger logger = Logger.getLogger(KafkaDelayedProducerRunnable.class.getSimpleName());
    /**
     * Kafka producer to produce the expired events to.
     */
    private final Producer<byte[], byte[]> kafkaProducer;
    /**
     * The storage to read events from.
     */
    private final KVStorage kvStorage;
    /**
     * The event business object.
     */
    private final EventHandler eventHandler;
    /**
     * How old an event needs to be in order to be considered expired, e.g, if this value is 5 seconds, this would mean
     * an event would be considered expired after 5 seconds.
     */
    private final Duration expiryAge;
    /**
     * The topics to send the events to.
     */
    private final Collection<String> sinkTopics;

    /**
     * The constructor for this runnable.
     *
     * @param kafkaProducer The Kafka producer to send events to.
     * @param kvStorage The storage to read events from.
     * @param eventHandler The event business object.
     * @param expiryAge How old an event needs to be in order to be considered expired.
     * @param sinkTopics The topics to send expired events to.
     */
    public KafkaDelayedProducerRunnable(
            final Producer<byte[], byte[]> kafkaProducer,
            final KVStorage kvStorage,
            final EventHandler eventHandler,
            final Duration expiryAge,
            final Collection<String> sinkTopics) {
        this.kafkaProducer = kafkaProducer;
        this.kvStorage = kvStorage;
        this.eventHandler = eventHandler;
        this.expiryAge = expiryAge;
        this.sinkTopics = sinkTopics;
    }

    /**
     * Runs the pipeline.
     */
    @Override
    public void run() {
        logger.info("Polling storage for expired events.");

        try {
            kvStorage
                    .StreamValues()
                    .map(this::extractPayload)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(this::validatePayload)
                    .map(this::sendPayload)
                    .forEach(this::deleteFromStorage);
        } catch (final Throwable e) {
            logger.warning(String.format("Failed parsing pipeline: %s", e.getMessage()));
        }
    }

    /**
     * This task tries to delete the given key from the storage.
     * It receives a pair composed by bool and a byte[], where the bool should be if the given key was successfully sent
     * to Kafka.
     *
     * @param pair Containing a bool and byte[], where the boolean represents if the value of the given key was sent to
     *             Kafka or not.
     */
    private void deleteFromStorage(final Pair<Boolean, byte[]> pair) {
        if(pair.getValue0()){
            try {
                kvStorage.DeleteValue(pair.getValue1());
            } catch (KVStorageException e) {
                logger.warning(String.format("Failed to delete value with key %s: %s", new String(pair.getValue1()), e.getMessage()));
            }
        } else {
            logger.warning(String.format("Unsent key %s", new String(pair.getValue1())));
        }
    }

    /**
     * Sends the given payload to Kafka.
     *
     * @param payload A pair where the first val is the key and the second the key value to be sent to Kafka.
     * @return A pair containing if the given payload has been sent successfully, and the key of the payload.
     */
    private Pair<Boolean, byte[]> sendPayload(final Pair<byte[], KeyValue> payload) {
        final KeyValue keyValueEvent = payload.getValue1();
        final Optional<Event> eventOpt = eventHandler.unmarshallEvent(keyValueEvent.getValue());

        if(eventOpt.isPresent()){
            final Event event = eventOpt.get();
            final long eventAge = Instant.now().toEpochMilli() - event.getTimestamp().toEpochMilli();

            if(eventAge > expiryAge.toMillis()) {
                logger.info(String.format("Sending to Kafka: %s", keyValueEvent));

                // Send the value to all Kafka topics.
                final Collection<ProducerRecord<byte[], byte[]>> producerRecords = sinkTopics
                        .stream()
                        .map(topic -> new ProducerRecord<>(topic, keyValueEvent.getKey(), keyValueEvent.getValue()))
                        .collect(Collectors.toCollection(ArrayList::new));
                // If one of the topics failed to send, we must try sending it again later.
                boolean sentToAllTopics = true;

                for(final ProducerRecord<byte[], byte[]> record : producerRecords) {
                    try {
                        synchronized (kafkaProducer) {
                            kafkaProducer.send(record).get();
                        }
                    } catch (final InterruptedException | ExecutionException e) {
                        logger.warning(String.format("Failed to send %s to Kafka topic %s: %s", keyValueEvent, record.topic(), e.getMessage()));
                        sentToAllTopics = false;

                        break;
                    }
                }

                return new Pair<>(sentToAllTopics, payload.getValue0());
            }
        }

        return new Pair<>(true, payload.getValue0());
    }

    /**
     * Validates the given payload to check if it contains an event with a timestamp.
     * The validation is done by the event handler.
     *
     * @param payload Payload to be validated.
     * @return If the payload is valid or not.
     */
    private boolean validatePayload(final Pair<byte[], KeyValue> payload) {
        if (payload == null || payload.getValue1() == null || payload.getValue0() == null) {
            return false;
        }

        final KeyValue keyValue = payload.getValue1();
        return eventHandler.validateEvent(keyValue.getValue());
    }

    /**
     * Extracts the payload event from the given KeyValue.
     * The idea is that the KeyValue has been extracted from the storage.
     *
     * @param keyValue KeyValue with a possible event.
     * @return Optional pair containing the key value event if it successfully extracted it.
     */
    private Optional<Pair<byte[], KeyValue>> extractPayload(final KeyValue keyValue) {
        try {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(keyValue.getValue());
            final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            final Object obj = objectInputStream.readObject();

            if (obj instanceof KeyValue) {
                final KeyValue keyValuePayload = (KeyValue) obj;

                return Optional.of(new Pair<>(keyValue.getKey(), keyValuePayload));
            } else {
                return Optional.empty();
            }
        } catch (final Throwable e) {
            logger.warning(String.format("Failed extracting payload: %s", e.getMessage()));

            return Optional.empty();
        }
    }
}
