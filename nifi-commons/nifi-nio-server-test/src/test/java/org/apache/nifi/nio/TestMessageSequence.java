package org.apache.nifi.nio;

import org.apache.nifi.nio.MessageSequenceTest.ClientRunnable;
import org.apache.nifi.nio.MessageSequenceTest.MessageSequenceHandlerProvider;
import org.apache.nifi.nio.stream.ChannelToBoundStream;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.nifi.nio.MessageSequenceTest.testMessageSequence;
import static org.junit.Assert.assertEquals;

public class TestMessageSequence {

    private static final Logger log = LoggerFactory.getLogger(TestMessageSequence.class);

    @Test
    public void testReadUTFStringFromBuffer() throws IOException {
        final String str = "abあc \n12";

        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(bos)) {

            // S2S clients writes UTF string using DataOutputStream.
            out.writeUTF(str);
            out.flush();

            // NIO S2S server has to get UTF string from a buffer.
            final ByteBuffer buffer = ByteBuffer.wrap(bos.toByteArray());
            assertEquals("The first 2 bytes represents the length of UTF data", 12, buffer.limit());
            final short utfLength = buffer.getShort();
            assertEquals(10, utfLength);

            // Construct a string.
            byte[] chars = new byte[utfLength];
            buffer.get(chars);
            final String s = new String(chars, StandardCharsets.UTF_8);

            assertEquals(str, s);
        }

    }

    private static class ExampleMessageSequence implements MessageSequence {
        // Implementation should have state like this.
        final AtomicInteger messageCount = new AtomicInteger(0);
        byte clientId;

        @Override
        public void clear() {
            log.info("Server finished the sequence. Clearing its status");
            MessageSequence.super.clear();
            messageCount.set(0);
            clientId = 0;
        }

        final ReadBytes firstMessage = new ReadBytes(1, bytes -> {
            log.info("Server received the 1st {} bytes", bytes.length);
            assertEquals(1, bytes.length);
            clientId = bytes[0];
            messageCount.incrementAndGet();
        });

        final ReadBytes secondMessage = new ReadBytes(2, bytes -> {
            log.info("Server received the 2nd {} bytes from client {}", bytes.length, clientId);
            assertEquals(2, bytes.length);
            if (clientId == 1) {
                assertEquals(20, bytes[0]);
                assertEquals(21, bytes[1]);
            } else {
                assertEquals(25, bytes[0]);
                assertEquals(26, bytes[1]);
            }
            messageCount.incrementAndGet();
        });

        final ReadUTF readUTF = new ReadUTF(value -> {
            log.info("Server received the UTF from client {}: {}", clientId, value);
            if (clientId == 1) {
                assertEquals("message from client 1", value);
            } else {
                assertEquals("message from client 2", value);
            }
            messageCount.incrementAndGet();
        });

        final WriteBytes serverResponse1 = new WriteBytes(
            () -> clientId == 1 ? new byte[]{1, 2, 3} : new byte[]{10, 20, 30}, () -> {
                messageCount.incrementAndGet();
                log.info("Server written the response to client {}", clientId);
            });

        final MessageAction[] actions = {firstMessage, secondMessage, serverResponse1, readUTF};

        @Override
        public MessageAction getNextAction() {
            final int i = messageCount.get();
            if (i == actions.length) {
                // Finished.
                return null;
            }
            return actions[i];
        }

        @Override
        public boolean isDone() {
            return 4 == messageCount.get();
        }
    }

    @Test
    public void testClientServerCommunication() throws Throwable {

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            ExampleMessageSequence::new,
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client1 = (in, out) -> {
            // client id
            out.write(1);

            // 2nd message
            out.write(20);
            out.write(21);

            // receive response.
            assertEquals(1, in.read());
            assertEquals(2, in.read());
            assertEquals(3, in.read());

            // 3rd message
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF("message from client 1");
        };

        testMessageSequence(handlerProvider, client1);
    }

    @Test
    public void testClientServerCommunicationRepeated() throws Throwable {

        final int numOfSequences = 2;

        final AtomicInteger finishedSequenceCount = new AtomicInteger(0);
        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            ExampleMessageSequence::new,
            sequence -> {
                final int i = finishedSequenceCount.incrementAndGet();
                log.info("Server finished sequence {}", i);
                if (i == numOfSequences) {
                    latch.countDown();
                }
            },
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client2 = (in, out) -> {
            // The same client socket can send multiple sequences.
            for (int i = 0; i < numOfSequences; i++) {
                // client id
                out.write(2);

                // 2nd message
                out.write(new byte[]{25, 26});

                // receive response.
                assertEquals(10, in.read());
                assertEquals(20, in.read());
                assertEquals(30, in.read());

                // 3rd message
                final DataOutputStream dos = new DataOutputStream(out);
                dos.writeUTF("message from client 2");
            }
            log.info("Client has finished.");
        };

        testMessageSequence(handlerProvider, client2);
    }

    @Test
    public void testClientServerCommunicationMultiClients() throws Throwable {

        // client1 (1) + client2 (3) = 4
        final int numOfSequences = 4;

        final AtomicInteger finishedSequenceCount = new AtomicInteger(0);

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            ExampleMessageSequence::new,
            sequence -> {
                final int i = finishedSequenceCount.incrementAndGet();
                log.info("Server finished sequence {}", i);
                if (i == numOfSequences) {
                    latch.countDown();
                }
            },
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client1 = (in, out) -> {
            // client id
            out.write(1);

            // 2nd message
            out.write(20);
            out.write(21);

            // receive response.
            assertEquals(1, in.read());
            assertEquals(2, in.read());
            assertEquals(3, in.read());

            // 3rd message
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF("message from client 1");
        };

        ClientRunnable client2 = (in, out) -> {
            // The same client socket can send multiple sequences.
            for (int i = 0; i< 3; i++) {
                // client id
                out.write(2);

                // 2nd message
                out.write(new byte[]{25, 26});

                // receive response.
                assertEquals(10, in.read());
                assertEquals(20, in.read());
                assertEquals(30, in.read());

                // 3rd message
                final DataOutputStream dos = new DataOutputStream(out);
                dos.writeUTF("message from client 2");
            }
        };

        testMessageSequence(handlerProvider, client1, client2);
    }

    private class ExampleTypeLengthValueSequence implements MessageSequence {

        private final MessageAction readType = new ReadUTF(type -> this.type = type);
        private final MessageAction readLength = new ReadInt(length -> this.length = length);
        private final ChannelToBoundStream streamData;
        private final WriteBytes writeResponse = new WriteBytes(() -> new byte[]{0}, () -> writtenResponse = true);

        @AutoCleared
        private String type;
        @AutoCleared
        private Integer length;
        @AutoCleared
        private boolean writtenResponse;

        private ExampleTypeLengthValueSequence(ChannelToBoundStream streamData) {
            this.streamData = streamData;
        }

        @Override
        public MessageAction getNextAction() {
            if (type == null) {
                return readType;
            }
            if (length == null) {
                return readLength;
            }
            if (!streamData.isDone()) {
                return streamData;
            }
            if (!writtenResponse) {
                return writeResponse;
            }
            return null;
        }

        @Override
        public void clear() {
            type = null;
            length = null;

            readType.clear();
            readLength.clear();
        }
    }

    private class ExampleMultiMessageSequence implements MultiMessageSequence {

        private final Consumer<InputStream> onReceivingDataStart;
        private MessageSequence handshake = new ExampleMessageSequence();
        private ExampleTypeLengthValueSequence receiveData = new ExampleTypeLengthValueSequence(
            new ChannelToBoundStream<>(2, 30_000, ExampleMultiMessageSequence::getDataSizeToRead)
        );

        private boolean startedStreamThread = false;

        private ExampleMultiMessageSequence(Consumer<InputStream> onReceivingDataStart) {
            this.onReceivingDataStart = onReceivingDataStart;
        }

        private int getDataSizeToRead() {
            if (receiveData == null || receiveData.length == null) {
                throw new IllegalStateException("Data size is not known yet.");
            }
            return receiveData.length;
        }

        @Override
        public MessageSequence getNextSequence() {
            if (receiveData.streamData.getInputStream() != null && !startedStreamThread) {
                // To emulate the way NiFi S2S consumes incoming data, receive data using inputStream from another thread.
                onReceivingDataStart.accept(receiveData.streamData.getInputStream());
                startedStreamThread = true;
            }
            if (!handshake.isDone()) {
                return  handshake;
            }
            if (!receiveData.isDone()) {
                return receiveData;
            }
            return null;
        }

        @Override
        public void clear() {
            handshake.clear();
            receiveData.clear();
        }
    }

    @Test
    public void testMultiSequenceClientServerCommunication() throws Throwable {

        final ExecutorService threadPool = Executors.newFixedThreadPool(3);

        MessageSequenceHandlerProvider handlerProvider = (serverAddress, latch, errorHandler) -> new MessageSequenceHandler<>(
            getClass().getSimpleName(),
            () -> new ExampleMultiMessageSequence(inputStream -> {
                // Simulate NiFi's RemoteGroupPort onTrigger. Start this thread when receiving data is started.
                threadPool.submit(() -> {
                    try {
                        log.info("Reading data using input stream. {}", inputStream);

                        final byte[] expected = {0, 1, 2, 3, 4};
                        int read = 0;
                        for (int b; (b = inputStream.read()) > -1; read++) {
                            log.info("Read {}th bytes {}", read, b);
                            assertEquals(expected[read], b);
                        }

                        log.info("Server received {} bytes data", read);
                        assertEquals(expected.length, read);
                    } catch (Throwable e) {
                        errorHandler.accept(e);
                    }
                });

            }),
            sequence -> latch.countDown(),
            (sequence, e) -> errorHandler.accept(e));

        ClientRunnable client = (in, out) -> {
            // client id
            out.write(1);

            // 2nd message
            out.write(20);
            out.write(21);

            // receive response.
            assertEquals(1, in.read());
            assertEquals(2, in.read());
            assertEquals(3, in.read());

            // 3rd message
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF("message from client 1");

            // Sending data
            dos.writeUTF("TestData");
            dos.writeInt(5);
            dos.write(new byte[]{0,1,2,3,4});

            // receive final response
            assertEquals(0, in.read());
        };

        MessageSequenceTest.testMessageSequence(handlerProvider, client);
    }
}
