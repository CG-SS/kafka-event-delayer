package cgss.dev.pipeline;

import cgss.dev.model.EventHandler;
import cgss.dev.storage.KVStorage;
import cgss.dev.storage.memory.MemoryStorage;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class KafkaSinkThreadTest {

    @Test
    public void run_SinkValues() {
        final List<String> topics = Arrays.asList("topic-1", "topic-2");
        final List<TopicPartition> topicPartitions = topics
                .stream()
                .map(topic -> new TopicPartition(topic, 0))
                .collect(Collectors.toCollection(ArrayList::new));
        final Duration pollDuration = Duration.ofSeconds(5);

        final MockConsumer<byte[], byte[]> kafkaConsumerMock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumerMock.assign(topicPartitions);
        topicPartitions.forEach(topicPartition -> kafkaConsumerMock.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L)));

        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
        final String timestampFieldName = "timestamp";
        final Gson gson = new Gson();
        final String eventPayload = String.format("{\"%s\": \"%s\", \"test\": true}", timestampFieldName, dateTimeFormatter.format(Instant.now()));

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        topicPartitions.forEach(topicPartition -> recordsMap.put(topicPartition, Collections.singletonList(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 0, topicPartition.topic().getBytes(), eventPayload.getBytes()))));
        kafkaConsumerMock.schedulePollTask(() -> recordsMap.values().forEach(r -> r.forEach(kafkaConsumerMock::addRecord)));

        final ConcurrentHashMap<byte[], byte[]> concurrentHashMap = new ConcurrentHashMap<>();
        final KVStorage kvStorage = new MemoryStorage(concurrentHashMap);

        Logger.getLogger(KafkaSinkThread.class.getSimpleName()).setLevel(Level.OFF);
        final KafkaSinkThread kafkaSinkThread = new KafkaSinkThread(
                kafkaConsumerMock,
                pollDuration,
                kvStorage,
                new EventHandler(timestampFieldName, gson, dateTimeFormatter)
        );
        kafkaSinkThread.start();

        try {
            Thread.sleep(Duration.ofMillis(100).toMillis());
        } catch (InterruptedException e) {
            kafkaSinkThread.interrupt();
            throw new RuntimeException(e);
        }

        kafkaSinkThread.interrupt();

        final boolean savedAllValues = kvStorage
                .StreamValues()
                .allMatch(keyValue -> new String(keyValue.getValue()).contains(eventPayload));

        Assert.assertTrue(savedAllValues);
    }

}
