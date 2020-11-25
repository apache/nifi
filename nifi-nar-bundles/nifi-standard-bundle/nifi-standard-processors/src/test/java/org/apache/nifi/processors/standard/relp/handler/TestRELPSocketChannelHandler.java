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
package org.apache.nifi.processors.standard.relp.handler;


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
import org.apache.nifi.processors.standard.relp.event.RELPMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
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

public class TestRELPSocketChannelHandler {

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
        channelHandlerFactory = new RELPSocketChannelHandlerFactory<>();

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
    public void testBasicHandling() throws IOException, InterruptedException {
        final List<String> messages = new ArrayList<>();
        messages.add("1 syslog 20 this is message 1234\n");
        messages.add("2 syslog 22 this is message 456789\n");
        messages.add("3 syslog 21 this is message ABCDE\n");

        run(messages);
        Assert.assertEquals(messages.size(), events.size());

        boolean found1 = false;
        boolean found2 = false;
        boolean found3 = false;

        TestEvent event;
        while((event = events.poll()) != null) {
            Map<String,String> metadata = event.metadata;
            Assert.assertTrue(metadata.containsKey(RELPMetadata.TXNR_KEY));

            final String txnr = metadata.get(RELPMetadata.TXNR_KEY);
            if (txnr.equals("1")) {
                found1 = true;
            } else if (txnr.equals("2")) {
                found2 = true;
            } else if (txnr.equals("3")) {
                found3 = true;
            }
        }

        Assert.assertTrue(found1);
        Assert.assertTrue(found2);
        Assert.assertTrue(found3);
    }

    @Test
    public void testLotsOfFrames() throws IOException, InterruptedException {
        final String baseMessage = " syslog 19 this is message ";
        final List<String> messages = new ArrayList<>();

        for (int i=100; i < 1000; i++) {
            messages.add(i + baseMessage + i + "\n");
        }

        run(messages);
        Assert.assertEquals(messages.size(), events.size());
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
                    buffer.put(messages.get(i).getBytes(charset));
                    buffer.flip();

                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    Thread.sleep(1);
                }
            }

            // wait up to 25 seconds to verify the responses
            long timeout = 25000;
            long startTime = System.currentTimeMillis();
            while (events.size() < messages.size() && (System.currentTimeMillis() - startTime < timeout)) {
                Thread.sleep(100);
            }

            // should have gotten an event for each message sent
            Assert.assertEquals(messages.size(), events.size());

        } finally {
            // stop the dispatcher thread and ensure we shut down handler threads
            dispatcher.close();
        }
    }

    // Test event to produce from the data
    private static class TestEvent implements Event<SocketChannel> {

        private byte[] data;
        private Map<String,String> metadata;

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
