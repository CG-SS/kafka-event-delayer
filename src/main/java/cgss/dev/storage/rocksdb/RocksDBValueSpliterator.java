package cgss.dev.storage.rocksdb;

import cgss.dev.storage.KeyValue;
import org.rocksdb.RocksIterator;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * A spliterator that wraps a given RocksIterator and provides a spliterator for key values.
 * Notice that this spliterator is not meant to be used concurrently.
 */
class RocksDBValueSpliterator implements Spliterator<KeyValue> {
    /**
     * The original iterator.
     */
    private final RocksIterator rocksIterator;

    /**
     * Constructor for the RocksDBValueSpliterator.
     *
     * @param rocksIterator The original iterator.
     */
    public RocksDBValueSpliterator(final RocksIterator rocksIterator) {
        this.rocksIterator = rocksIterator;
    }

    /**
     * Try to continue to consume the original iterator and feed the new value into the stream.
     *
     * @param consumer The action.
     * @return If the spliterator is still valid.
     */
    @Override
    public boolean tryAdvance(Consumer<? super KeyValue> consumer) {
        if(!rocksIterator.isValid()){
            return false;
        }

        consumer.accept(new KeyValue(rocksIterator.key(), rocksIterator.value()));
        rocksIterator.next();

        return rocksIterator.isValid();
    }

    /**
     * Does nothing, as the original iterator is not splittable.
     *
     * @return null
     */
    @Override
    public Spliterator<KeyValue> trySplit() {
        return null;
    }

    /**
     * As this is not meant to be concurrent, it returns Long.MAX_VALUE.
     *
     * @return Long.MAX_VALUE
     */
    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    /**
     * Returns the spliterator characteristics.
     *
     * @return Spliterator.NONNULL | Spliterator.IMMUTABLE
     */
    @Override
    public int characteristics() {
        return Spliterator.NONNULL | Spliterator.IMMUTABLE;
    }
}
