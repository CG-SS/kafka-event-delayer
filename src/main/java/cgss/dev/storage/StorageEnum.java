package cgss.dev.storage;

/**
 * Enum used to define which storage backend is supposed to be used.
 * These are the possible values for 'storage.type'.
 */
public enum StorageEnum {
    NOP, MEMORY, ROCKSDB
}
