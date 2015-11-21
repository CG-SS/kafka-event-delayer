package cgss.dev.storage.rocksdb;

import cgss.dev.storage.KeyValue;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.Spliterator;
import java.util.function.Consumer;

class RocksDBValueSpliterator implements Spliterator<KeyValue> {
    private final RocksIterator rocksIterator;
    private final long estimatedSize;

    public RocksDBValueSpliterator(final RocksIterator rocksIterator) {
        long estimatedSize = 0;
        for(rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()){
            estimatedSize++;
        }

        rocksIterator.seekToFirst();

        this.estimatedSize = estimatedSize;
        this.rocksIterator = rocksIterator;
    }

    @Override
    public boolean tryAdvance(Consumer<? super KeyValue> consumer) {
        if(!rocksIterator.isValid()){
            return false;
        }

        consumer.accept(new KeyValue(rocksIterator.key(), rocksIterator.value()));
        rocksIterator.next();

        return rocksIterator.isValid();
    }

    @Override
    public Spliterator<KeyValue> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return estimatedSize;
    }

    @Override
    public int characteristics() {
        return Spliterator.NONNULL | Spliterator.IMMUTABLE;
    }
}
