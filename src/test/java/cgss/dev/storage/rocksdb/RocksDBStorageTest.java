package cgss.dev.storage.rocksdb;

import cgss.dev.storage.KVStorageException;
import cgss.dev.storage.KeyValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

public class RocksDBStorageTest {

    @Test
    public void saveValue_SavesValue() throws KVStorageException {
        final RocksDB rocksDBMock = Mockito.mock(RocksDB.class);

        final byte[] testKey = "test-key".getBytes(StandardCharsets.UTF_8);
        final byte[] testValue = "test-value".getBytes(StandardCharsets.UTF_8);

        final RocksDBStorage storage = new RocksDBStorage(rocksDBMock);

        storage.SaveValue(new KeyValue(testKey, testValue));
    }

    @Test
    public void saveValue_ThrowsError() throws RocksDBException {
        final RocksDB rocksDBMock = Mockito.mock(RocksDB.class);

        final byte[] testKey = "test-key".getBytes(StandardCharsets.UTF_8);
        final byte[] testValue = "test-value".getBytes(StandardCharsets.UTF_8);

        Mockito.doAnswer(invocationOnMock -> {
            throw new RocksDBException("RocksDB error");
        }).when(rocksDBMock).put(testKey, testValue);

        final RocksDBStorage storage = new RocksDBStorage(rocksDBMock);

        try {
            storage.SaveValue(new KeyValue(testKey, testValue));
            Assert.fail("Should've escalated the error!");
        } catch (KVStorageException e) {
            // If it catches, it's correct.
        }
    }

    @Test
    public void deleteValue_DeletesValue() throws KVStorageException {
        final RocksDB rocksDBMock = Mockito.mock(RocksDB.class);

        final byte[] testKey = "test-key".getBytes(StandardCharsets.UTF_8);

        final RocksDBStorage storage = new RocksDBStorage(rocksDBMock);

        storage.DeleteValue(testKey);
    }

    @Test
    public void deleteValue_ThrowsError() throws RocksDBException {
        final RocksDB rocksDBMock = Mockito.mock(RocksDB.class);

        final byte[] testKey = "test-key".getBytes(StandardCharsets.UTF_8);

        Mockito.doAnswer(invocationOnMock -> {
            throw new RocksDBException("RocksDB error");
        }).when(rocksDBMock).delete(testKey);

        final RocksDBStorage storage = new RocksDBStorage(rocksDBMock);

        try {
            storage.DeleteValue(testKey);
            Assert.fail("Should've escalated the error!");
        } catch (KVStorageException e) {
            // If it catches, it's correct.
        }
    }

    @Test
    public void streamValues_StreamValues() {
        final RocksDB rocksDBMock = Mockito.mock(RocksDB.class);
        final Collection<KeyValue> keyValueCollection = Arrays.asList(
                new KeyValue("test-key-1".getBytes(), "test-value-1".getBytes()),
                new KeyValue("test-key-2".getBytes(), "test-value-2".getBytes()),
                new KeyValue("test-key-3".getBytes(), "test-value-3".getBytes())
        );

        keyValueCollection.forEach(keyValue -> {
            try {
                Mockito.when(rocksDBMock.get(keyValue.getKey())).thenReturn(keyValue.getValue());
            } catch (final RocksDBException e) {
                Assert.fail(e.getMessage());
            }
        });

        final RocksDBStorage storage = new RocksDBStorage(rocksDBMock);

        storage.StreamValues().forEach(keyValue -> Assert.assertTrue(keyValueCollection.contains(keyValue)));
    }
}
