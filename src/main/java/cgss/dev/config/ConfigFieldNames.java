package cgss.dev.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.Collection;

/**
 * This interface holds all the used configuration values.
 * We use those when trying to fetch the config values from the properties file and environment variables.
 * If passing those using environment variables, it will look for each of then on the environment.
 */
public interface ConfigFieldNames {
    String SINK_TOPICS_FIELD_NAME = "sink.topics";
    String SOURCE_TOPICS_FIELD_NAME = "source.topics";

    String CONSUMER_BOOTSTRAP_SERVERS_FIELD_NAME = "consumer.bootstrap.servers";

    String CONSUMER_POLL_INTERVAL = "consumer.poll.time";
    String GROUP_ID_FIELD_NAME = ConsumerConfig.GROUP_ID_CONFIG;

    String PRODUCER_BOOTSTRAP_SERVERS_FIELD_NAME = "producer.bootstrap.servers";
    String PRODUCER_POLL_INTERVAL = "producer.poll.time";

    String TIMESTAMP_FIELD_NAME = "timestamp.field.name";

    String STORAGE_TYPE = "storage.type";

    String ROCKSDB_PATH = "rocksdb.path";
    String EXPIRY_AGE = "expiry.age";

    Collection<String> ALL_FIELD_NAMES = Arrays.asList(
            SINK_TOPICS_FIELD_NAME,
            SOURCE_TOPICS_FIELD_NAME,
            CONSUMER_BOOTSTRAP_SERVERS_FIELD_NAME,
            GROUP_ID_FIELD_NAME,
            PRODUCER_BOOTSTRAP_SERVERS_FIELD_NAME,
            CONSUMER_POLL_INTERVAL,
            PRODUCER_POLL_INTERVAL,
            TIMESTAMP_FIELD_NAME,
            STORAGE_TYPE,
            ROCKSDB_PATH,
            EXPIRY_AGE
    );
}
