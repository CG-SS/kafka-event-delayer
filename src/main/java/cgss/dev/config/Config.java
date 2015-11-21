package cgss.dev.config;

import cgss.dev.storage.StorageEnum;
import org.javatuples.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.logging.Logger;

/**
 * The configuration POJO for the application.
 * Here you can find all the configuration items for the whole app.
 */
public class Config implements ConfigFieldNames {
    /**
     * The default logger for this class.
     */
    private static final Logger logger = Logger.getLogger(Config.class.getSimpleName());
    /**
     * The topics to send the expired events to.
     */
    private final Collection<String> sinkTopics;
    /**
     * The topics to consume the events to be expired.
     */
    private final Collection<String> sourceTopics;
    /**
     * The time formatter to be used to convert the given field to an instant.
     * Currently, we only support ISO_INSTANT.
     */
    private final DateTimeFormatter dateTimeFormatter;
    /**
     * The bootstrap servers for the consumer. We can technically have two bootstrap servers, one for a producer, the
     * other for the consumer.
     */
    private final String consumerBootstrapServers;
    /**
     * The interval between event consumptions.
     */
    private final Duration consumerPollInternal;
    /**
     * The group ID for the consumer.
     */
    private final String consumerGroupId;
    /**
     * The producer bootstrap servers.
     */
    private final String producerBootstrapServers;
    /**
     * The field name that will store the timestamp in an event.
     */
    private final String timestampFieldName;
    /**
     * If using RocksDB, the path for the database.
     */
    private final String rocksDbPath;
    /**
     * The interval for checking for expired events.
     */
    private final Duration producerPollInternal;
    /**
     * How old an event needs to be in order to be considered expired.
     */
    private final Duration expiryAge;
    /**
     * The type of storage to be used.
     */
    private final StorageEnum kvStorage;

    /**
     * Config constructor.
     *
     * @param sinkTopics The topics to send the expired events to.
     * @param sourceTopics The topics to consume the events from.
     * @param dateTimeFormatter The formatter to format the timestamp on events.
     * @param consumerBootstrapServers The bootstrap servers for the consumer.
     * @param consumerGroupId The group ID for the consumer.
     * @param producerBootstrapServers The bootstrap servers for the producer.
     * @param consumerPollInternal The interval between event consumptions.
     * @param producerPollInternal The interval for checking for expired events.
     * @param timestampFieldName The field name that will store the timestamp in an event.
     * @param kvStorage The type of storage to be used.
     * @param rocksDbPath If using RocksDB, the path for the database.
     * @param expiryAge How old an event needs to be in order to be considered expired.
     */
    private Config(Collection<String> sinkTopics, Collection<String> sourceTopics, DateTimeFormatter dateTimeFormatter, String consumerBootstrapServers, String consumerGroupId, String producerBootstrapServers, Duration consumerPollInternal, Duration producerPollInternal, String timestampFieldName, StorageEnum kvStorage, String rocksDbPath, Duration expiryAge) {
        this.sinkTopics = sinkTopics;
        this.sourceTopics = sourceTopics;
        this.dateTimeFormatter = dateTimeFormatter;
        this.consumerBootstrapServers = consumerBootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.producerBootstrapServers = producerBootstrapServers;
        this.consumerPollInternal = consumerPollInternal;
        this.producerPollInternal = producerPollInternal;
        this.timestampFieldName = timestampFieldName;
        this.kvStorage = kvStorage;
        this.rocksDbPath = rocksDbPath;
        this.expiryAge = expiryAge;
    }

    /**
     * Loads the config using the default configuration file.
     *
     * @return The configuration POJO.
     * @throws InvalidConfigException If any of the given configuration have an invalid field.
     */
    public static Config load() throws InvalidConfigException {
        return Config.load("./local.properties");
    }

    /**
     * Loads the config using the provided file path.
     * First, we try to load from the file, if the file doesn't exist, we try to fetch it from environment variables.
     *
     * @param propertiesFilePath The path to the configuration file.
     * @return The configuration POJO.
     * @throws InvalidConfigException  If any of the given configuration have an invalid field.
     */
    public static Config load(final String propertiesFilePath) throws InvalidConfigException {
        Properties properties;

        final File propertiesFile = new File(propertiesFilePath);

        logger.info(String.format("Trying to load configs from file: %s", propertiesFile.getAbsolutePath()));

        // First we try to load from the file.
        if (propertiesFile.exists() && propertiesFile.isFile()) {
            try {
                properties = Config.loadPropertiesFromFile(propertiesFile);
            } catch (final IOException e) {
                throw new InvalidConfigException(String.format("Failed loading config from file: %s", e.getMessage()));
            }
        } else {
            // If it doesn't exist, we assume we need to load from env. vars.
            logger.info("Loading config from environment variables.");

            properties = Config.loadPropertiesFromEnv();
        }

        return Config.fromProperties(properties);
    }

    /**
     * Loads the properties from the given file.
     *
     * @param propertiesFile The file that contains the properties.
     * @return Properties that will be used as the config.
     * @throws IOException If it fails to read from the given file.
     */
    private static Properties loadPropertiesFromFile(final File propertiesFile) throws IOException {
        try (final FileInputStream fileInputStream = new FileInputStream(propertiesFile)) {
            final Properties properties = new Properties();
            properties.load(fileInputStream);

            return properties;
        }
    }

    /**
     * Loads the properties from the environment.
     * It will use the 'ALL_FIELD_NAMES' variable to try and load each of the properties.
     *
     * @return Properties loaded from the environment to be used on the config.
     */
    private static Properties loadPropertiesFromEnv() {
        final Properties properties = new Properties();

        ALL_FIELD_NAMES
                .stream()
                .map(fieldName -> new Pair<>(fieldName, Optional.ofNullable(System.getenv(fieldName)).orElse("")))
                .forEach(pair -> properties.put(pair.getValue0(), pair.getValue1()));

        return properties;
    }

    /**
     * Unwraps the config from properties.
     * It tries to get all the values from the Properties and validate each. If some of them are not valid, it won't
     * return immediately, it will collect all the errors.
     *
     * @param properties The properties to fetch the configs from.
     * @return Config loaded from the given properties file.
     * @throws InvalidConfigException If any of the given properties are invalid.
     */
    private static Config fromProperties(final Properties properties) throws InvalidConfigException {
        final Collection<String> missingFields = new ArrayList<>();
        final Collection<String> errorMessages = new ArrayList<>();

        final String sinkTopicsVal = extractAndValidateIfEmpty(properties, SINK_TOPICS_FIELD_NAME, missingFields);
        final Collection<String> sinkTopics = Arrays.asList(sinkTopicsVal.split(","));

        final String sourceTopicVal = extractAndValidateIfEmpty(properties, SOURCE_TOPICS_FIELD_NAME, missingFields);
        final Collection<String> sourceTopics = Arrays.asList(sourceTopicVal.split(","));

        final String timestampFieldName = extractAndValidateIfEmpty(properties, TIMESTAMP_FIELD_NAME, missingFields);

        final String consumerBootstrapServerVal = extractAndValidateIfEmpty(properties, CONSUMER_BOOTSTRAP_SERVERS_FIELD_NAME, missingFields);
        final String consumerGroupIdVal = extractAndValidateIfEmpty(properties, GROUP_ID_FIELD_NAME, missingFields);

        final String producerBootstrapServerVal = extractAndValidateIfEmpty(properties, PRODUCER_BOOTSTRAP_SERVERS_FIELD_NAME, missingFields);

        final String consumerPollIntervalVal = extractAndValidateIfEmpty(properties, CONSUMER_POLL_INTERVAL, missingFields);
        Duration consumerPollInterval = Duration.ZERO;
        try {
            consumerPollInterval = Duration.parse(consumerPollIntervalVal);
        } catch (DateTimeParseException e) {
            errorMessages.add(String.format("could not parse '%s' consumer poll duration: %s", consumerPollIntervalVal, e.getMessage()));
        }

        final String producerPollIntervalVal = extractAndValidateIfEmpty(properties, PRODUCER_POLL_INTERVAL, missingFields);
        Duration producerPollInterval = Duration.ZERO;
        try {
            producerPollInterval = Duration.parse(producerPollIntervalVal);
        } catch (DateTimeParseException e) {
            errorMessages.add(String.format("could not parse '%s' producer poll duration: %s", producerPollIntervalVal, e.getMessage()));
        }

        final String expiryAgeVal = extractAndValidateIfEmpty(properties, EXPIRY_AGE, missingFields);
        Duration expiryAge = Duration.ZERO;
        try {
            expiryAge = Duration.parse(expiryAgeVal);
        } catch (DateTimeParseException e) {
            errorMessages.add(String.format("could not parse '%s' expiry age duration: %s", expiryAgeVal, e.getMessage()));
        }

        final String storageTypeVal = extractAndValidateIfEmpty(properties, STORAGE_TYPE, missingFields);
        StorageEnum storageType = StorageEnum.NOP;
        try {
            storageType = StorageEnum.valueOf(storageTypeVal);
        } catch (IllegalArgumentException e) {
            errorMessages.add(String.format("unknown storage type %s", storageTypeVal));
        }

        final String rocksDbPath = extractAndValidateIfEmpty(properties, ROCKSDB_PATH, missingFields);

        if (!missingFields.isEmpty()) {
            final String missingFieldsJoin = String.join(",", missingFields);
            final String errMsg = String.format("missing required env config values: %s", missingFieldsJoin);

            errorMessages.add(errMsg);
        }

        if (!errorMessages.isEmpty()) {
            final String errorsMessageJoin = String.join(";", errorMessages);
            final String errMsg = String.format("One or more configuration issues found: %s", errorsMessageJoin);

            throw new InvalidConfigException(errMsg);
        }

        return new Config(
                sinkTopics,
                sourceTopics,
                DateTimeFormatter.ISO_INSTANT,
                consumerBootstrapServerVal,
                consumerGroupIdVal,
                producerBootstrapServerVal,
                consumerPollInterval,
                producerPollInterval,
                timestampFieldName,
                storageType,
                rocksDbPath,
                expiryAge
        );
    }

    /**
     * Extracts the given property from the Properties file and check if it's empty, that is, an empty string.
     * If it is, it will insert it in to the 'missingFields' array.
     *
     * @param properties The properties file to extract the field from.
     * @param property The property name.
     * @param missingFields The missing field array to mark all the missing fields.
     * @return The actual value of the given property.
     */
    private static String extractAndValidateIfEmpty(final Properties properties, final String property, final Collection<String> missingFields) {
        final String propertyVal = properties.getProperty(property);
        if(propertyVal == null || propertyVal.isEmpty()){
            missingFields.add(property);
        }

        return Optional.ofNullable(propertyVal).orElse("");
    }

    /**
     * Get the producer bootstrap servers.
     *
     * @return The producer bootstrap servers.
     */
    public String getProducerBootstrapServers() {
        return producerBootstrapServers;
    }

    /**
     * Get the sink topics.
     *
     * @return The sink topics.
     */
    public Collection<String> getSinkTopics() {
        return sinkTopics;
    }

    /**
     * Get the source topics.
     *
     * @return The source topics.
     */
    public Collection<String> getSourceTopics() {
        return sourceTopics;
    }

    /**
     * Get the consumer bootstrap servers.
     *
     * @return The consumer bootstrap servers.
     */
    public String getConsumerBootstrapServers() {
        return consumerBootstrapServers;
    }

    /**
     * Get the consumer group ID.
     *
     * @return The consumer group ID.
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     * Get the date time formatter.
     *
     * @return The date time formatter.
     */
    public DateTimeFormatter getDateTimeFormatter() {
        return dateTimeFormatter;
    }

    /**
     * Get the consumer poll interval.
     *
     * @return The consumer poll interval.
     */
    public Duration getConsumerPollInternal() {
        return consumerPollInternal;
    }

    /**
     * Get the producer polling time.
     *
     * @return The producer polling time.
     */
    public Duration getProducerPollInternal() {
        return producerPollInternal;
    }

    /**
     * Get the timestamp field name.
     *
     * @return The timestamp field name.
     */
    public String getTimestampFieldName() {
        return timestampFieldName;
    }

    /**
     * Get the configured key value storage enum.
     *
     * @return The configured key value storage enum.
     */
    public StorageEnum getKvStorage() {
        return kvStorage;
    }

    /**
     * Get the RocksDB database path.
     *
     * @return The RocksDB database path.
     */
    public String getRocksDbPath() {
        return rocksDbPath;
    }

    /**
     * Get the event expiry age.
     *
     * @return The event expiry age.
     */
    public Duration getExpiryAge() {
        return expiryAge;
    }
}
