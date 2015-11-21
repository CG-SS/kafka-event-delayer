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

public class RocksDBStorage implements KVStorage {

    private final RocksDB rocksDB;

    public RocksDBStorage(final String dbPath) throws RocksDBException {
        final Options options = new Options()
                .setCreateIfMissing(true);
        rocksDB = RocksDB.open(options, dbPath);
    }

    @Override
    public void SaveValue(final KeyValue keyValue) throws KVStorageException {
        try {
            rocksDB.put(keyValue.getKey(), keyValue.getValue());
        } catch (final RocksDBException e) {
            throw new KVStorageException(e);
        }
    }

    @Override
    public void DeleteValue(final byte[] key) throws KVStorageException {
        try {
            rocksDB.remove(key);
        } catch (RocksDBException e) {
            throw new KVStorageException(e);
        }
    }

    @Override
    public Stream<KeyValue> StreamValues() {
        final RocksIterator rocksIterator = rocksDB.newIterator();

        return StreamSupport.stream(new RocksDBValueSpliterator(rocksIterator), false);
    }
}
