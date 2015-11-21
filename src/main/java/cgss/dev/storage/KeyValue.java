package cgss.dev.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

/**
 * KeyValue is a POJO that stores a key value pair of bytes.
 * The intention is for it to be used with the KVStorage.
 */
public class KeyValue implements Serializable {

    /**
     * The key for the key value.
     */
    private final byte[] key;
    /**
     * The value for the key value.
     */
    private final byte[] value;

    /**
     * Constructor for the KeyValue POJO.
     *
     * @param key The key.
     * @param value The value.
     */
    public KeyValue(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the value of this key value pair.
     *
     * @return The value.
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Returns the key of this key value pair.
     *
     * @return The key.
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Converts the key value to String.
     * If null, will populate the respective value with the string "null".
     *
     * @return The string representation of KeyValue.
     */
    @Override
    public String toString() {
        final byte[] keyBytes = Optional.ofNullable(key).orElse("null".getBytes());
        final byte[] valueBytes = Optional.ofNullable(value).orElse("null".getBytes());

        return "KeyValue{" +
                "key=" + new String(keyBytes) +
                ", value=" + new String(valueBytes) +
                '}';
    }

    /**
     * Equality comparison function.
     *
     * @param o The other object to compare to.
     * @return If the given object is the same as this object.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyValue keyValue = (KeyValue) o;

        if (!Arrays.equals(key, keyValue.key)) return false;
        return Arrays.equals(value, keyValue.value);
    }

    /**
     * Generic hashcode function.
     * Uses key and value to generate a hashcode.
     *
     * @return The hashcode for the given key and value.
     */
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }


}
