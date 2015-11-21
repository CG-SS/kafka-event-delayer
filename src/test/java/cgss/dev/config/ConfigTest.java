package cgss.dev.config;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConfigTest implements ConfigFieldNames {

    @Test
    public void load_LoadsFromFile() throws InvalidConfigException {
        Logger.getLogger(Config.class.getSimpleName()).setLevel(Level.OFF);
        final Config config = Config.load("./src/test/java/cgss/dev/config/test.properties");

        Assert.assertNotNull(config);
    }

    @Test
    public void load_FailsLoadingFromFile() {
        Logger.getLogger(Config.class.getSimpleName()).setLevel(Level.OFF);
        try {
            Config.load("./src/test/java/cgss/dev/config/missing.properties");
        } catch (InvalidConfigException e) {
            return;
        }
        Assert.fail("Should've thrown an InvalidConfigException!");
    }

    @Test
    public void load_LoadsFromEnv() throws Exception {
        Logger.getLogger(Config.class.getSimpleName()).setLevel(Level.OFF);

        setEnv(SINK_TOPICS_FIELD_NAME, "sink-topic");
        setEnv(SOURCE_TOPICS_FIELD_NAME, "source-topic");
        setEnv(CONSUMER_BOOTSTRAP_SERVERS_FIELD_NAME, "localhost:9092");
        setEnv(CONSUMER_KEY_DESERIALIZER_FIELD_NAME, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        setEnv(CONSUMER_VALUE_DESERIALIZER_FIELD_NAME, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        setEnv(CONSUMER_POLL_INTERVAL, "PT5S");
        setEnv(GROUP_ID_FIELD_NAME, "group-id");
        setEnv(PRODUCER_BOOTSTRAP_SERVERS_FIELD_NAME, "localhost:9092");
        setEnv(PRODUCER_KEY_SERIALIZER_FIELD_NAME, "org.apache.kafka.common.serialization.ByteArraySerializer");
        setEnv(PRODUCER_VALUE_SERIALIZER_FIELD_NAME, "org.apache.kafka.common.serialization.ByteArraySerializer");
        setEnv(PRODUCER_POLL_INTERVAL, "PT5S");
        setEnv(TIMESTAMP_FIELD_NAME, "timestamp");
        setEnv(STORAGE_TYPE, "ROCKSDB");
        setEnv(ROCKSDB_PATH, "./db");
        setEnv(EXPIRY_AGE, "PT5S");

        final Config config = Config.load();

        Assert.assertNotNull(config);
    }

    @Test
    public void load_FailsLoadingFromEnv() {
        Logger.getLogger(Config.class.getSimpleName()).setLevel(Level.OFF);
        try {
            Config.load();
        } catch (InvalidConfigException e) {
            return;
        }
        Assert.fail("Should've thrown an InvalidConfigException!");
    }

    private static void setEnv(String key, String value) throws Exception {
        Map<String, String> env = System.getenv();
        Class<?> cl = env.getClass();
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writableEnv = (Map<String, String>) field.get(env);
        writableEnv.put(key, value);
    }

}
