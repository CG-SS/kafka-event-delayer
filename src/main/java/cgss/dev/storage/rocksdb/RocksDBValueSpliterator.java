package cgss.dev.storage.rocksdb;

import cgss.dev.storage.KeyValue;
import org.rocksdb.RocksIterator;

import java.util.Spliterator;
import java.util.function.Consumer;

class RocksDBValueSpliterator implements Spliterator<KeyValue> {
    private final RocksIterator rocksIterator;

    public RocksDBValueSpliterator(final RocksIterator rocksIterator) {
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
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.NONNULL | Spliterator.IMMUTABLE;
    }
}
