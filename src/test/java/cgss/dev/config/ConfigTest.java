package cgss.dev.config;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

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

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void load_LoadsFromEnv() throws Exception {
        Logger.getLogger(Config.class.getSimpleName()).setLevel(Level.OFF);

        environmentVariables.set(SINK_TOPICS_FIELD_NAME, "sink-topic");
        environmentVariables.set(SOURCE_TOPICS_FIELD_NAME, "source-topic");
        environmentVariables.set(CONSUMER_BOOTSTRAP_SERVERS_FIELD_NAME, "localhost:9092");
        environmentVariables.set(CONSUMER_POLL_INTERVAL, "PT5S");
        environmentVariables.set(GROUP_ID_FIELD_NAME, "group-id");
        environmentVariables.set(PRODUCER_BOOTSTRAP_SERVERS_FIELD_NAME, "localhost:9092");
        environmentVariables.set(PRODUCER_POLL_INTERVAL, "PT5S");
        environmentVariables.set(TIMESTAMP_FIELD_NAME, "timestamp");
        environmentVariables.set(STORAGE_TYPE, "ROCKSDB");
        environmentVariables.set(ROCKSDB_PATH, "./db");
        environmentVariables.set(EXPIRY_AGE, "PT5S");

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

}
