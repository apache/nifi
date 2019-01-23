package org.apache.nifi.nio;

import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.MessageSequenceTest.ServerStarted;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;

public class TestReadInt {

    private static final Logger LOG = LoggerFactory.getLogger(TestReadInt.class);

    private void testNormal(boolean secure) throws Throwable {
        final AtomicInteger expectedCount = new AtomicInteger(0);
        final ReadInt readInt = new ReadInt(n -> {
            final int i = expectedCount.incrementAndGet();
            LOG.info("Read {} at {}", n, i);
            Assert.assertEquals((long) n, (long) i);
        });
        final WriteBytes writeResponse = new WriteBytes(() -> new byte[]{0}, expectedCount::incrementAndGet);
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> () -> {
                if (expectedCount.get() == 2) {
                    return writeResponse;
                } else if (expectedCount.get() == 3) {
                    return null;
                }
                return readInt;
            },
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {

            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(1);
            dos.writeInt(2);

            final int response = in.read();
            Assert.assertEquals(0, response);
        };

        testMessageSequence(handlerProvider, new ServerStarted[0], new ClientRunnable[]{client}, secure);
    }

    @Test
    public void test() throws Throwable {
        testNormal(false);
    }

    @Test
    public void testSecure() throws Throwable {
        testNormal(true);
    }

}
