package org.apache.nifi.nio.stream;

import org.apache.nifi.nio.MessageSequenceHandler;
import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.MessageSequenceTest.ServerStarted;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStreamToChannel {

    private static final Logger LOG = LoggerFactory.getLogger(TestStreamToChannel.class);

    private void testOneSequence(boolean secure) throws Throwable {

        final StreamToChannel streamToChannel = new StreamToChannel(64, 30_000);

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> () -> {
                if (!streamToChannel.isDone()) {
                    return streamToChannel;
                }
                return null;
            },
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {
            final DataInputStream dis = new DataInputStream(in);
            assertEquals(1, dis.readInt());
            assertEquals(2, dis.readInt());
            assertEquals(0, dis.available());
        };

        // Another component is running on a different thread, producing the data using OutputStream wrapping the non-blocking channel.
        ServerStarted dataProducer = new ServerStarted("data-producer", serverAddress -> {
            final OutputStream outputStream = streamToChannel.getOutputStream();
            final DataOutputStream dos = new DataOutputStream(outputStream);
            dos.writeInt(1);
            dos.writeInt(2);
            dos.flush();
            streamToChannel.complete();
        });

        testMessageSequence(handlerProvider, new ServerStarted[]{dataProducer}, new ClientRunnable[]{client}, secure);
    }

    private void testTwoSequences(boolean secure) throws Throwable {

        final StreamToChannel streamToChannel = new StreamToChannel(64, 30_000);

        final AtomicInteger sequenceCount = new AtomicInteger(0);
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> () -> {
                if (!streamToChannel.isDone()) {
                    return streamToChannel;
                }

                return null;
            },
            sequence -> {
                if (sequenceCount.incrementAndGet() == 2) {
                    latch.countDown();
                }
            },
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {
            final DataInputStream dis = new DataInputStream(in);
            assertEquals(1, dis.readInt());
            assertEquals(2, dis.readInt());

            assertEquals(3, dis.readInt());
            assertEquals(4, dis.readInt());

        };

        ServerStarted dataProducer = new ServerStarted("data-producer", serverAddress -> {
            OutputStream outputStream = streamToChannel.getOutputStream();
            DataOutputStream dos = new DataOutputStream(outputStream);
            dos.writeInt(1);
            dos.writeInt(2);
            dos.flush();

            assertFalse("StreamChannel is still piping data via stream.", streamToChannel.isDone());
            Future<Boolean> complete = streamToChannel.complete();
            try {
                assertTrue(complete.get(3, TimeUnit.SECONDS));
            } catch (InterruptedException|ExecutionException|TimeoutException e) {
                throw new RuntimeException(e);
            }

            outputStream = streamToChannel.getOutputStream();
            dos = new DataOutputStream(outputStream);
            dos.writeInt(3);
            dos.writeInt(4);
            dos.flush();
            complete = streamToChannel.complete();
            try {
                assertTrue(complete.get(3, TimeUnit.SECONDS));
            } catch (InterruptedException|ExecutionException|TimeoutException e) {
                throw new RuntimeException(e);
            }
        });

        testMessageSequence(handlerProvider, new ServerStarted[]{dataProducer}, new ClientRunnable[]{client}, secure);
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
