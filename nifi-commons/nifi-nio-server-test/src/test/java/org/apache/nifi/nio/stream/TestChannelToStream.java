package org.apache.nifi.nio.stream;

import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.MessageSequence;
import org.apache.nifi.nio.MessageSequenceHandler;
import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.MessageSequenceTest.ServerStarted;
import org.apache.nifi.nio.Rewind;
import org.apache.nifi.nio.WriteBytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;

public class TestChannelToStream {

    private static final Logger LOG = LoggerFactory.getLogger(TestChannelToStream.class);

    private static final long TIMEOUT_MILLIS = 30_000;

    private void testOneSequence(boolean secure) throws Throwable {

        final ChannelToStream channelToStream = new ChannelToStream(64, TIMEOUT_MILLIS);
        final Rewind rewind = new Rewind(channelToStream);

        final AtomicBoolean done = new AtomicBoolean(false);
        final WriteBytes completed = new WriteBytes(() -> new byte[]{0}, () -> done.set(true));
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> () -> {
                if (!channelToStream.isDone()) {
                    return channelToStream;
                } else if (!rewind.isDone()) {
                    return rewind;
                } else if (!done.get()) {
                    return completed;
                }
                return null;
            },
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(1);
            dos.writeInt(2);

            final int result = in.read();
            Assert.assertEquals(0, result);
        };

        // Another component is running on a different thread, consuming the data using InputStream wrapping the non-blocking channel.
        ServerStarted dataConsumer = new ServerStarted("data-consumer", serverAddress -> {
            InputStream inputStream = channelToStream.getInputStream();
            DataInputStream dis = new DataInputStream(inputStream);
            Assert.assertEquals(1, dis.readInt());
            Assert.assertEquals(2, dis.readInt());
            channelToStream.complete();
        });

        testMessageSequence(handlerProvider, new ServerStarted[]{dataConsumer}, new ClientRunnable[]{client}, secure);
    }

    private void testTwoSequences(boolean secure) throws Throwable {

        final ChannelToStream channelToStream = new ChannelToStream(64, TIMEOUT_MILLIS);
        final AtomicInteger sequenceCount = new AtomicInteger(0);
        final CountDownLatch firstSequence = new CountDownLatch(1);
        final CountDownLatch secondSequence = new CountDownLatch(1);
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> {
                final MessageSequence messageSequence = new MessageSequence() {
                    private final Rewind rewind = new Rewind(channelToStream);

                    @Override
                    public void clear() {
                        channelToStream.clear();
                        rewind.clear();
                        firstSequence.countDown();
                    }

                    @Override
                    public MessageAction getNextAction() {
                        if (!channelToStream.isDone()) {
                            return channelToStream;

                        } else if (!rewind.isDone()) {
                            return rewind;
                        }

                        return null;
                    }
                };
                return messageSequence;
            },
            sequence -> {
                final int i = sequenceCount.incrementAndGet();
                if (i == 2) {
                    secondSequence.countDown();
                    latch.countDown();
                }
            },
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {
            final DataOutputStream dos = new DataOutputStream(out);
            // 2 ints for the 1st interaction.
            dos.writeInt(1);
            dos.writeInt(2);

            // 2 ints for the 2nd interaction.
            // These inputs can be piped to the InputStream before the data for the 1st interaction is processed at the consuming side.
            // In that case, these bytes are already piped from Channel reading buffer to the piped stream.
            // The next action "Rewind" is used to back-fill the already piped data into the Channel reading buffer.
            dos.writeInt(3);
            dos.writeInt(4);

            // Wait server finishes reading the data.
            try {
                secondSequence.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        };

        ServerStarted dataConsumer = new ServerStarted("data-consumer", serverAddress -> {
            InputStream inputStream = channelToStream.getInputStream();
            DataInputStream dis = new DataInputStream(inputStream);
            Assert.assertEquals(1, dis.readInt());
            Assert.assertEquals(2, dis.readInt());

            Assert.assertFalse("ChannelToStream is still piping data via stream.", channelToStream.isDone());
            channelToStream.complete();

            try {
                firstSequence.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            inputStream = channelToStream.getInputStream();
            dis = new DataInputStream(inputStream);
            Assert.assertEquals(3, dis.readInt());
            Assert.assertEquals(4, dis.readInt());
            channelToStream.complete();
        });

        testMessageSequence(handlerProvider, new ServerStarted[]{dataConsumer}, new ClientRunnable[]{client}, secure);
    }

    @Test
    public void testOneSequence() throws Throwable {
        testOneSequence(false);
    }

    @Test
    public void testOneSequenceSecure() throws Throwable {
        testOneSequence(true);
    }

    @Test
    public void testTwoSequences() throws Throwable {
        testTwoSequences(false);
    }

    @Test
    public void testTwoSequencesSecure() throws Throwable {
        testTwoSequences(true);
    }
}
