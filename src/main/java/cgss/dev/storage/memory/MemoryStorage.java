package cgss.dev.storage.memory;

import cgss.dev.storage.KVStorage;
import cgss.dev.storage.KeyValue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * A memory backed KVStorage.
 * Internally, we just use a concurrent hashmap. Only recommended if you don't care about losing durability for the sake
 * of latency.
 */
public class MemoryStorage implements KVStorage {
    /**
     * The map where the key value pairs are stored.
     */
    private final ConcurrentHashMap<byte[], byte[]> concurrentHashMap;

    /**
     * Constructor for injecting an existing concurrent hash map.
     *
     * @param concurrentHashMap A concurrent hashmap to be used to save the events.
     */
    public MemoryStorage(final ConcurrentHashMap<byte[], byte[]> concurrentHashMap) {
        this.concurrentHashMap = concurrentHashMap;
    }

    /**
     * Default constructor.
     */
    public MemoryStorage() {
        this(new ConcurrentHashMap<>());
    }

    /**
     * Saves the given key value on the concurrent hash map.
     *
     * @param keyValue Object representing a key value.
     */
    @Override
    public void SaveValue(final KeyValue keyValue) {
        concurrentHashMap.put(keyValue.getKey(), keyValue.getValue());
    }

    /**
     * Deletes the value associated with the given key from the concurret hash map.
     *
     * @param key The key for the given value.
     */
    @Override
    public void DeleteValue(byte[] key) {
        concurrentHashMap.remove(key);
    }

    /**
     * Stream the values currently stored in the hash map.
     *
     * @return A stream containing all the current values.
     */
    @Override
    public Stream<KeyValue> StreamValues() {
        return concurrentHashMap.entrySet().stream().map(entry -> new KeyValue(entry.getKey(), entry.getValue()));
    }
}
