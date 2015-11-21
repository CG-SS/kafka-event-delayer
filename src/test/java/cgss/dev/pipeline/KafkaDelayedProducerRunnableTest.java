package cgss.dev.pipeline;

import cgss.dev.model.EventHandler;
import cgss.dev.storage.KVStorage;
import cgss.dev.storage.KVStorageException;
import cgss.dev.storage.KeyValue;
import cgss.dev.storage.memory.MemoryStorage;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaDelayedProducerRunnableTest {

    @Test
    public void run_ProducesToKafka() throws IOException, KVStorageException {
        final String topic = "test";
        final int expirySeconds = 60;
        final String timestampFieldName = "timestamp";
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
        final String eventPayload = String.format("{\"%s\": \"%s\", \"test\": true}", timestampFieldName, dateTimeFormatter.format(Instant.now().minusSeconds(expirySeconds)));

        final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        final KVStorage kvStorage = new MemoryStorage();

        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(new KeyValue(null, eventPayload.getBytes()));
        final byte[] eventBytes = byteStream.toByteArray();

        kvStorage.SaveValue(new KeyValue("0".getBytes(), eventBytes));

        final EventHandler eventHandler = new EventHandler(timestampFieldName, new Gson(), DateTimeFormatter.ISO_INSTANT);

        Logger.getLogger(KafkaDelayedProducerRunnable.class.getSimpleName()).setLevel(Level.OFF);
        final KafkaDelayedProducerRunnable kafkaDelayedProducerRunnable = new KafkaDelayedProducerRunnable(
                mockProducer,
                kvStorage,
                eventHandler,
                Duration.ofSeconds(expirySeconds),
                Collections.singletonList(topic)
        );
        kafkaDelayedProducerRunnable.run();

        Assert.assertEquals(0, kvStorage.StreamValues().count());

        final List<ProducerRecord<byte[], byte[]>> producerRecords = mockProducer.history();

        Assert.assertEquals(1, producerRecords.size());
        Assert.assertEquals(eventPayload, new String(producerRecords.get(0).value()));
    }

}
