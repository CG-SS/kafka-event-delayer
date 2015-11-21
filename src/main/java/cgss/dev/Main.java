package cgss.dev;

import cgss.dev.config.Config;
import cgss.dev.config.InvalidConfigException;
import cgss.dev.model.EventHandler;
import cgss.dev.pipeline.KafkaDelayedProducerRunnable;
import cgss.dev.pipeline.KafkaSinkThread;
import cgss.dev.storage.KVStorage;
import cgss.dev.storage.memory.MemoryStorage;
import cgss.dev.storage.nop.NopStorage;
import cgss.dev.storage.rocksdb.RocksDBStorage;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The main class for the application.
 * Here we initialize all the required configuration entries and start the sink and source pipelines.
 */
public class Main extends Thread {

    /**
     * Main app configuration, contains all the required configs for the application.
     */
    private final Config config;
    /**
     * The executor that will run the expiry check, defined by KafkaDelayedProducerRunnable.
     */
    private final ScheduledExecutorService executor;
    /**
     * Thread responsible for consuming events from Kafka and saving them to the given storage.
     */
    private final KafkaSinkThread kafkaSinkThread;
    /**
     * This represents the task that check for expired events on the given storage and sends them to Kafka.
     */
    private final KafkaDelayedProducerRunnable kafkaDelayedProducerRunnable;

    /**
     * Constructor for the main app.
     *
     * @param config The app configuration.
     * @throws RocksDBException If storage type is set RocksDB, will throw in case of failure to create a Rocks DB.
     */

    public Main(final Config config) throws RocksDBException {
        this.config = config;
        this.executor = Executors.newScheduledThreadPool(1);

        // Create the storage. Default RocksDB.
        KVStorage kvStorage;
        switch (config.getKvStorage()) {
            case NOP:
                kvStorage = new NopStorage();
                break;
            case MEMORY:
                kvStorage = new MemoryStorage();
                break;
            default:
                kvStorage = new RocksDBStorage(config.getRocksDbPath());
                break;
        }

        // The class name to be used as deserializer.
        final String deserializerClassName = ByteArrayDeserializer.class.getName();

        // Extracts the properties for the Kafka consumer.
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getConsumerBootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClassName);
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClassName);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroupId());
        final KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(new ArrayList<>(config.getSourceTopics()));

        // The business object for the event payload.
        final EventHandler eventHandler = new EventHandler(config.getTimestampFieldName(), new Gson(), config.getDateTimeFormatter());

        // Create the sink thread that will consume events from kafka and save it in the storage.
        this.kafkaSinkThread = new KafkaSinkThread(
                kafkaConsumer,
                config.getConsumerPollInternal(),
                kvStorage,
                eventHandler
        );

        // The class name to be used as serializer.
        final String serializerClassName = ByteArraySerializer.class.getName();

        // Extracts the properties for the Kafka producer.
        final Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProducerBootstrapServers());
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClassName);
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClassName);
        final KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(producerProperties);

        // Creates the task that will check for expired events on the storage and send them to Kafka.
        this.kafkaDelayedProducerRunnable = new KafkaDelayedProducerRunnable(
                kafkaProducer,
                kvStorage,
                eventHandler,
                config.getExpiryAge(),
                config.getSinkTopics()
        );
    }

    /**
     * Runs the main application.
     * This will start the consumer thread and schedule the producer task based on the given poll time.
     */
    @Override
    public void run() {
        kafkaSinkThread.start();
        executor.scheduleAtFixedRate(kafkaDelayedProducerRunnable, 0, config.getProducerPollInternal().toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Interrupts the main thread, shutting down the pipelines.
     */
    @Override
    public void interrupt() {
        super.interrupt();

        kafkaSinkThread.interrupt();
        executor.shutdown();
    }

    /**
     * The main function.
     * Creates the Main app and starts the flow.
     *
     * @param args Command line arguments. Not used.
     * @throws InvalidConfigException Will be thrown if an invalid configuration was detected either on the file or env. vars.
     * @throws RocksDBException Will be thrown if an invalid configuration was provided when using RocksDB as the storage provider.
     */
    public static void main(String[] args) throws InvalidConfigException, RocksDBException {
        final Config config = Config.load();

        final Main mainApp = new Main(config);
        mainApp.start();

        // If we get stopped, we need to close the pipelines.
        Runtime.getRuntime().addShutdownHook(new Thread(mainApp::interrupt));
    }

}