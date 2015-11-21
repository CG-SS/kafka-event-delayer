package cgss.dev.model;

import com.google.gson.Gson;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.format.DateTimeFormatter;
import java.util.Optional;

@RunWith(JUnitParamsRunner.class)
public class EventHandlerTest {

    static Gson gson = new Gson();

    @Test
    @Parameters({
            "timestamp, {\"timestamp\": \"2013-06-21T22:34:56.089716700Z\"}, true", // 2023-07-09T15:31:26.746882600Z
            "timestamp, {\"timestamp\": \"2014-06-20T21:34:56.089716700Z\"}, true",
            "time, {\"time\": \"2015-06-22T10:11:56.089716700Z\"}, true",
            "t, {\"t\": \"2012-06-22T16:11:56.089716700Z\"}, true",
            "time, {\"t\": \"2015-06-22T16:11:56.089716700Z\"}, false",
            "timestamp, {\"time\": \"2014-06-22T16:11:56.089716700Z\"}, false",
            "timestamp, {\"timestamp\": \"2014-06-20T61:34:56.089716700Z\"}, false",
    })
    public void validateEvent_ValidateEvents(final String timestampFieldName, final String eventJsonStr, final boolean expected) {
        final EventHandler eventHandler = new EventHandler(timestampFieldName, gson, DateTimeFormatter.ISO_INSTANT);

        final boolean validationResult = eventHandler.validateEvent(eventJsonStr.getBytes());

        Assert.assertEquals(expected, validationResult);
    }

    @Test
    @Parameters({
            "timestamp, {\"timestamp\": \"2013-06-21T22:34:56.089716700Z\"}, true",
            "timestamp, {\"timestamp\": \"2014-06-20T21:34:56.089716700Z\"}, true",
            "time, {\"time\": \"2015-06-22T10:11:56.089716700Z\"}, true",
            "t, {\"t\": \"2012-06-22T16:11:56.089716700Z\"}, true",
            "time, {\"t\": \"2015-06-22T16:11:56.089716700Z\"}, false",
            "timestamp, {\"time\": \"2014-06-22T16:11:56.089716700Z\"}, false",
            "timestamp, {\"timestamp\": \"2014-06-20T61:34:56.089716700Z\"}, false",
    })
    public void unmarshallEvent_UnmarshallsEvents(final String timestampFieldName, final String eventJsonStr, final boolean expected) {
        final EventHandler eventHandler = new EventHandler(timestampFieldName, gson, DateTimeFormatter.ISO_INSTANT);

        final Optional<Event> eventOpt = eventHandler.unmarshallEvent(eventJsonStr.getBytes());

        Assert.assertEquals(expected, eventOpt.isPresent());
    }

}
