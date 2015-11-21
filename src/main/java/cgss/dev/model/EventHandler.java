package cgss.dev.model;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * The event model business object.
 */
public class EventHandler {

    /**
     * Default logger for the event handler class.
     */
    private final static Logger logger = Logger.getLogger(EventHandler.class.getSimpleName());
    /**
     * The name of the timestamp field in the event. We will check the JSON for this field name. The idea is that you
     * can configure your own timestamp event name in the JSON.
     */
    private final String timestampFieldName;
    /**
     * The Gson instance to marshall and unmarshall JSON.
     */
    private final Gson gson;
    /**
     * The formatter for the timestamp field.
     * When we try to extract the timestamp value from the events, this formatter is used.
     */
    private final DateTimeFormatter dateTimeFormatter;

    /**
     * Constructor for the event handler.
     *
     * @param timestampFieldName The field name for the timestamp on the event.
     * @param gson The Gson JSON marshaller/unmarshaller.
     * @param dateTimeFormatter The formatter object to convert timestamps.
     */
    public EventHandler(String timestampFieldName, Gson gson, DateTimeFormatter dateTimeFormatter) {
        this.timestampFieldName = timestampFieldName;
        this.gson = gson;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    /**
     * Validates the given bytes for a possible events.
     *
     * @param possibleEvent A byte array representing a possible event.
     * @return If the given bytes are a valid event configured by this event handler.
     */
    public boolean validateEvent(final byte[] possibleEvent) {
        if (possibleEvent == null) {
            return false;
        }

        final boolean isValid = extractInstant(possibleEvent).isPresent();
        if (!isValid){
            logger.warning(String.format("Invalid event received: %s", new String(possibleEvent)));
        }

        return isValid;
    }

    /**
     * Tries to unmarshall the given possible event.
     *
     * @param possibleEvent The byte array for the event.
     * @return An optional containing the event if the given byte array was an event, empty otherwise.
     */
    public Optional<Event> unmarshallEvent(final byte[] possibleEvent) {
        final Optional<Instant> optionalInstant = extractInstant(possibleEvent);

        return optionalInstant.map(Event::new);
    }

    /**
     * Tries to extract the instant from the given byte array.
     *
     * @param possibleEvent The byte array for the event.
     * @return An optional containing the instant if the given byte array contains a valid one, empty otherwise.
     */
    private Optional<Instant> extractInstant(final byte[] possibleEvent) {
        try {
            final JsonObject jsonObject = gson.fromJson(new String(possibleEvent), JsonElement.class).getAsJsonObject();

            final JsonElement possibleJsonElement = jsonObject.get(timestampFieldName);
            if (possibleJsonElement == null) {
                return Optional.empty();
            }

            final String timestampStr = possibleJsonElement.getAsString();

            return Optional.ofNullable(Instant.from(dateTimeFormatter.parse(timestampStr)));
        } catch (final Throwable e) {
            logger.warning(String.format("Invalid instant: %s", e.getMessage()));

            return Optional.empty();
        }
    }

}
