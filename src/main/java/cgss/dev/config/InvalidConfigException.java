package cgss.dev.config;

/**
 * The exception thrown if any configuration values are incorrect or missing.
 */
public class InvalidConfigException extends Exception {
    /**
     * Constructor for the invalid configurations.
     *
     * @param message String that contains the configuration fields that are invalid.
     */
    public InvalidConfigException(final String message) {
        super(message);
    }

}
