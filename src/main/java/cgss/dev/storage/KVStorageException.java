package cgss.dev.storage;

/**
 * Exception thrown for KVStorage errors.
 */
public class KVStorageException extends Exception {
    /**
     * Constructor for KVStorageException.
     *
     * @param cause The actual cause.
     */
    public KVStorageException(final Throwable cause) {
        super(cause);
    }
}
