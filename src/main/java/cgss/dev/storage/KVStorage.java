package cgss.dev.storage;

import java.util.stream.Stream;

public interface KVStorage {

    void SaveValue(final KeyValue keyValue) throws KVStorageException;
    void DeleteValue(final byte[] key) throws KVStorageException;
    Stream<KeyValue> StreamValues();

}
