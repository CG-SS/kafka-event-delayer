package cgss.dev.storage.nop;

import cgss.dev.storage.KVStorage;
import cgss.dev.storage.KeyValue;

import java.util.stream.Stream;

/**
 * A no-operation storage.
 * Meant for testing and mocking.
 */
public class NopStorage implements KVStorage {

    /**
     * No-operation.
     *
     * @param keyValue Object representing a key value.
     */
    @Override
    public void SaveValue(final KeyValue keyValue) {}

    /**
     * No-operation.
     *
     * @param key The key for the given value.
     */
    @Override
    public void DeleteValue(byte[] key) {}

    /**
     * Returns an empty stream.
     *
     * @return Stream.empty()
     */
    @Override
    public Stream<KeyValue> StreamValues() {
        return Stream.empty();
    }
}
