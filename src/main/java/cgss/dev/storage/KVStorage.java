package cgss.dev.storage;

import java.util.stream.Stream;

/**
 * This interface represents a key value storage.
 * The idea is that, given a key, you would store a given value.
 * Currently, we never fetch one single value, due to the nature of the application, so we rely on streaming in order
 * to access the values in the key value storage.
 */
public interface KVStorage {

    /**
     * Saves a given value with the given key.
     *
     * @param keyValue Object representing a key value.
     * @throws KVStorageException If it fails to save.
     */
    void SaveValue(final KeyValue keyValue) throws KVStorageException;

    /**
     * Deletes the value associated with the given key, if it exists.
     *
     * @param key The key for the given value.
     * @throws KVStorageException If it fails to delete.
     */
    void DeleteValue(final byte[] key) throws KVStorageException;

    /**
     * Streams all the current pairs stored by this KV Storage.
     *
     * @return A stream containing the current KV storage at the moment of calling.
     */
    Stream<KeyValue> StreamValues();

}
