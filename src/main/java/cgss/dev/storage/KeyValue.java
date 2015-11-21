package cgss.dev.storage;

public class KeyValue {

    private final byte[] key;
    private final byte[] value;

    public KeyValue(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }


    public byte[] getValue() {
        return value;
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "key=" + new String(key) +
                ", value=" + new String(value) +
                '}';
    }
}
