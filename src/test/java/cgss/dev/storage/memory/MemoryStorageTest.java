package cgss.dev.storage.memory;

import cgss.dev.storage.KeyValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStorageTest {

    @Test
    public void saveValue_SavesValue() {
        final ConcurrentHashMap<byte[], byte[]> concurrentHashMap = new ConcurrentHashMap<>();
        final MemoryStorage memoryStorage = new MemoryStorage(concurrentHashMap);

        final Collection<KeyValue> keyValueCollection = Arrays.asList(
                new KeyValue("test-key-1".getBytes(), "test-value-1".getBytes()),
                new KeyValue("test-key-2".getBytes(), "test-value-2".getBytes()),
                new KeyValue("test-key-3".getBytes(), "test-value-3".getBytes())
        );

        keyValueCollection.forEach(memoryStorage::SaveValue);
        final boolean isMissingEntry = concurrentHashMap
                .entrySet()
                .stream()
                .map(entry -> new KeyValue(entry.getKey(), entry.getValue()))
                .map(keyValueCollection::contains)
                .anyMatch(contains -> !contains);

        Assert.assertFalse(isMissingEntry);
    }

    @Test
    public void deleteValue_DeletesValue() {
        final ConcurrentHashMap<byte[], byte[]> concurrentHashMap = new ConcurrentHashMap<>();

        final Collection<KeyValue> keyValueCollection = Arrays.asList(
                new KeyValue("test-key-1".getBytes(), "test-value-1".getBytes()),
                new KeyValue("test-key-2".getBytes(), "test-value-2".getBytes()),
                new KeyValue("test-key-3".getBytes(), "test-value-3".getBytes())
        );

        keyValueCollection.forEach(kv -> concurrentHashMap.put(kv.getKey(), kv.getValue()));

        final MemoryStorage memoryStorage = new MemoryStorage(concurrentHashMap);

        keyValueCollection.forEach(kv -> memoryStorage.DeleteValue(kv.getKey()));

        Assert.assertTrue(concurrentHashMap.isEmpty());
    }

    @Test
    public void streamValues_StreamsValues() {
        final ConcurrentHashMap<byte[], byte[]> concurrentHashMap = new ConcurrentHashMap<>();

        final Collection<KeyValue> keyValueCollection = Arrays.asList(
                new KeyValue("test-key-1".getBytes(), "test-value-1".getBytes()),
                new KeyValue("test-key-2".getBytes(), "test-value-2".getBytes()),
                new KeyValue("test-key-3".getBytes(), "test-value-3".getBytes()),
                new KeyValue("test-key-4".getBytes(), "test-value-4".getBytes())
        );

        keyValueCollection.forEach(kv -> concurrentHashMap.put(kv.getKey(), kv.getValue()));

        final MemoryStorage memoryStorage = new MemoryStorage(concurrentHashMap);

        final boolean isMissingEntry = memoryStorage
                .StreamValues()
                .map(keyValueCollection::contains)
                .anyMatch(contains -> !contains);

        Assert.assertFalse(isMissingEntry);
    }


}
