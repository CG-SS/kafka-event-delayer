package cgss.dev.storage.nop;

import cgss.dev.storage.KeyValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Stream;

public class NopStorageTest {

    @Test
    public void streamValues_ReturnEmpty() {
        final NopStorage nopStorage = new NopStorage();

        nopStorage.SaveValue(new KeyValue("test-1".getBytes(), "value-1".getBytes()));
        nopStorage.SaveValue(new KeyValue("test-2".getBytes(), "value-2".getBytes()));

        Assert.assertEquals(Stream.empty().count(), nopStorage.StreamValues().count());
    }

}
