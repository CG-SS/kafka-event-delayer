package cgss.dev.storage.rocksdb;

import cgss.dev.storage.KVStorage;
import cgss.dev.storage.KVStorageException;
import cgss.dev.storage.KeyValue;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A KVStorage backed by Rocks DB.
 * It will save the values on disk inside a RocksDB instance.
 */
public class RocksDBStorage implements KVStorage {

    /**
     * The actual RocksDB instance.
     */
    private final RocksDB rocksDB;

    /**
     * Constructor that takes a path where a RocksDB instance will be created.
     *
     * @param dbPath The path where the RocksDB instance will be created.
     * @throws RocksDBException If fails to create a RocksDB instance with the given path.
     */
    public RocksDBStorage(final String dbPath) throws RocksDBException {
        this(RocksDB.open(new Options().setCreateIfMissing(true), dbPath));
    }

    /**
     * Constructor that accepts a RocksDB.
     *
     * @param rocksDB RocksDB instance to be used.
     */
    public RocksDBStorage(final RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    /**
     * Saves the given KeyValue pair.
     *
     * @param keyValue Object representing a key value.
     * @throws KVStorageException If it fails to save the key value pair.
     */
    @Override
    public void SaveValue(final KeyValue keyValue) throws KVStorageException {
        try {
            rocksDB.put(keyValue.getKey(), keyValue.getValue());
        } catch (final RocksDBException e) {
            throw new KVStorageException(e);
        }
    }

    /**
     * Deletes a value given the provided key.
     *
     * @param key The key for the given value.
     * @throws KVStorageException If it fails to delete the key value pair.
     */
    @Override
    public void DeleteValue(final byte[] key) throws KVStorageException {
        try {
            rocksDB.remove(key);
        } catch (RocksDBException e) {
            throw new KVStorageException(e);
        }
    }

    /**
     * Streams the current stored key value pairs.
     * It uses a RocksDBValueSpliterator to wrap a RocksIterator in order to provide a stream.
     *
     * @return A stream containing the stored key value pairs.
     */
    @Override
    public Stream<KeyValue> StreamValues() {
        final RocksIterator rocksIterator = rocksDB.newIterator();

        return StreamSupport.stream(new RocksDBValueSpliterator(rocksIterator), false);
    }
}
