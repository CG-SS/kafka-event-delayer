package cgss.dev.storage.rocksdb;

import cgss.dev.storage.KeyValue;
import org.junit.Test;
import org.mockito.Mockito;
import org.junit.Assert;
import org.rocksdb.RocksIterator;

import java.util.*;
import java.util.stream.Collectors;

public class RocksDBValueSpliteratorTest {

    @Test
    public void tryAdvance_IteratesOverAllValues() {
        final RocksIterator rocksIteratorMock = Mockito.mock(RocksIterator.class);
        final List<KeyValue> keyValueList = Arrays.asList(
                new KeyValue("test-key-1".getBytes(), "test-value-1".getBytes()),
                new KeyValue("test-key-2".getBytes(), "test-value-2".getBytes()),
                new KeyValue("test-key-3".getBytes(), "test-value-3".getBytes())
        );

        Mockito.when(rocksIteratorMock.value()).thenReturn(keyValueList.get(0).getValue(), keyValueList.get(1).getValue(), keyValueList.get(2).getValue());
        Mockito.when(rocksIteratorMock.key()).thenReturn(keyValueList.get(0).getKey(), keyValueList.get(1).getKey(), keyValueList.get(2).getKey());
        Mockito.when(rocksIteratorMock.isValid()).thenReturn(true, true, true, false);

        final RocksDBValueSpliterator spliterator = new RocksDBValueSpliterator(rocksIteratorMock);

        final Collection<KeyValue> collectedValues = new ArrayList<>();
        while(spliterator.tryAdvance(collectedValues::add));

        Assert.assertEquals(keyValueList, collectedValues);

        Mockito.verify(rocksIteratorMock, Mockito.times(4)).isValid();
        Mockito.verify(rocksIteratorMock, Mockito.times(3)).value();
        Mockito.verify(rocksIteratorMock, Mockito.times(3)).key();
    }

    @Test
    public void trySplit_ReturnNull() {
        final RocksIterator rocksIteratorMock = Mockito.mock(RocksIterator.class);
        final RocksDBValueSpliterator spliterator = new RocksDBValueSpliterator(rocksIteratorMock);

        Assert.assertNull(spliterator.trySplit());
    }

    @Test
    public void estimateSize_ReturnNoSize() {
        final RocksIterator rocksIteratorMock = Mockito.mock(RocksIterator.class);
        final RocksDBValueSpliterator spliterator = new RocksDBValueSpliterator(rocksIteratorMock);

        Assert.assertEquals(Long.MAX_VALUE, spliterator.estimateSize());
    }

    @Test
    public void characteristics_ChecksNonParallel() {
        final RocksIterator rocksIteratorMock = Mockito.mock(RocksIterator.class);
        final RocksDBValueSpliterator spliterator = new RocksDBValueSpliterator(rocksIteratorMock);

        Assert.assertEquals(Spliterator.NONNULL | Spliterator.IMMUTABLE, spliterator.characteristics());
    }
}
