package cgss.dev.model;

import java.time.Instant;

/**
 * POJO that represents an event.
 * The idea is that the event needs to have at least a timestamp, so we can check if it's expired.
 */
public class Event {

    /**
     * The timestamp of the event to check if it's expired.
     */
    private final Instant timestamp;

    /**
     * Event constructor.
     *
     * @param timestamp A timestamp to be used for checking if it's expired.
     */
    public Event(final Instant timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the event timestamp.
     *
     * @return The event timestamp.
     */
    public Instant getTimestamp() {
        return timestamp;
    }
}
