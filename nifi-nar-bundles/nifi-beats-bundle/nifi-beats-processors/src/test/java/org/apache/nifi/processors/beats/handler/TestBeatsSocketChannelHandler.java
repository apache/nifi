/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.beats.handler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.ByteBufferPool;
import org.apache.nifi.processor.util.listen.dispatcher.ByteBufferSource;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processors.beats.event.BeatsMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;




public class TestBeatsSocketChannelHandler {
    private EventFactory<TestEvent> eventFactory;
    private ChannelHandlerFactory<TestEvent,AsyncChannelDispatcher> channelHandlerFactory;
    private ByteBufferSource byteBufferSource;
    private BlockingQueue<TestEvent> events;
    private ComponentLog logger = Mockito.mock(ComponentLog.class);
    private int maxConnections;
    private SSLContext sslContext;
    private Charset charset;
    private ChannelDispatcher dispatcher;

    @Before
    public void setup() {
        eventFactory = new TestEventHolderFactory();
        channelHandlerFactory = new BeatsSocketChannelHandlerFactory<>();

        byteBufferSource = new ByteBufferPool(1, 4096);

        events = new LinkedBlockingQueue<>();
        logger = Mockito.mock(ComponentLog.class);

        maxConnections = 1;
        sslContext = null;
        charset = StandardCharsets.UTF_8;

        dispatcher = new SocketChannelDispatcher<>(eventFactory, channelHandlerFactory, byteBufferSource, events, logger,
                maxConnections, sslContext, charset);

    }

    @Test
    public void testWiredJsonHandling() throws IOException, InterruptedException {
        final String singleJsonFrame = "324a000000010000002d7b226d657373616765223a2022746573742d636f6e74656e7422" +
                "2c20226669656c64223a202276616c7565227d";
        final List<String> messages = new ArrayList<>();
        messages.add(singleJsonFrame);

        run(messages);

        // Check for the 1 frames (from the hex string above) are back...
        Assert.assertEquals(1, events.size());

        TestEvent event;
        while((event = events.poll()) != null) {
            Map<String, String> metadata = event.metadata;
            Assert.assertTrue(metadata.containsKey(BeatsMetadata.SEQNUMBER_KEY));

            final String seqNum = metadata.get(BeatsMetadata.SEQNUMBER_KEY);
            final String line = new String(event.getData());
            Assert.assertTrue(seqNum.equals("1"));
            Assert.assertEquals("{\"message\": \"test-content\", \"field\": \"value\"}", line);
        }
    }

    @Test
    public void testCompressedJsonHandling() throws IOException, InterruptedException {
        final String multipleJsonFrame = "3243000000E27801CC91414BC3401085477FCA3B6F" +
                "93EEB6A5B8A71E3CF5ECC98BECC6491AC86643665290903FAE17A982540F8237E7F" +
                "80D3C78EF734722BA21A2B71987C41A9E8306F819FA32303CBADCC020725078D46D" +
                "C791836231D0EB7FDB0F933EE9354A2C129A4B44F8B8AF94197D4817CE7CCF67189" +
                "CB2E80F74E651DADCC36357D8C2623138689B5834A4011E6E6DF7ABB55DAD770F76" +
                "E3B7777EBB299CB58F30903C8D15C3A33CE5C465A8A74ACA2E3792A7B1E25259B4E" +
                "87203835CD7C20ABF5FDC91886E89E8F58F237CEEF2EF1A5967BEFBFBD54F8C3162" +
                "790F0000FFFF6CB6A08D";

        final List<String> messages = new ArrayList<>();
        messages.add(multipleJsonFrame);

        run(messages);

        // Check for the 2 frames (from the hex string above) are back...
        Assert.assertEquals(2, events.size());

        boolean found1 = false;
        boolean found2 = false;


        TestEvent event;
        while((event = events.poll()) != null) {
            Map<String, String> metadata = event.metadata;
            Assert.assertTrue(metadata.containsKey(BeatsMetadata.SEQNUMBER_KEY));

            final String seqNum = metadata.get(BeatsMetadata.SEQNUMBER_KEY);
            final String line = new String(event.getData());

            if (seqNum.equals("1") && line.contains("\"message\":\"aaaaaa\"")) {
                found1 = true;
            }
            if (seqNum.equals("2") && line.contains("\"message\":\"bbbb\"")) {
                found2 = true;
            }
        }
        Assert.assertTrue(found1 && found2);
    }


    protected void run(List<String> messages) throws IOException, InterruptedException {
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            // starts the dispatcher listening on port 0 so it selects a random port
            dispatcher.open(null, 0, 4096);

            // starts a thread to run the dispatcher which will accept/read connections
            Thread dispatcherThread = new Thread(dispatcher);
            dispatcherThread.start();


            // create a client connection to the port the dispatcher is listening on
            final int realPort = dispatcher.getPort();
            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(new InetSocketAddress("localhost", realPort));
                Thread.sleep(100);

                // send the provided messages
                for (int i=0; i < messages.size(); i++) {
                    buffer.clear();
                    buffer.put(DatatypeConverter.parseHexBinary(messages.get(i)));
                    buffer.flip();

                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    Thread.sleep(1);
                }
            }

            // wait up to 10 seconds to verify the responses
            long timeout = 10000;
            long startTime = System.currentTimeMillis();
            while (events.size() < messages.size() && (System.currentTimeMillis() - startTime < timeout)) {
                Thread.sleep(100);
            }

        } finally {
            // stop the dispatcher thread and ensure we shut down handler threads
            dispatcher.close();
        }
    }

    // Test event to produce from the data
    private static class TestEvent implements Event<SocketChannel> {

        private byte[] data;
        private Map<String, String> metadata;

        public TestEvent(byte[] data, Map<String, String> metadata) {
            this.data = data;
            this.metadata = metadata;
        }

        @Override
        public String getSender() {
            return metadata.get(EventFactory.SENDER_KEY);
        }

        @Override
        public byte[] getData() {
            return data;
        }

        @Override
        public ChannelResponder<SocketChannel> getResponder() {
            return null;
        }
    }

    // Factory to create test events and send responses for testing
    private static class TestEventHolderFactory implements EventFactory<TestEvent> {

        @Override
        public TestEvent create(final byte[] data, final Map<String, String> metadata, final ChannelResponder responder) {
            return new TestEvent(data, metadata);
        }
    }
}
