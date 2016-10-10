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
package org.apache.nifi.processor.util.listen.dispatcher;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.event.EventQueue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Reads from the Datagram channel into an available buffer. If data is read then the buffer is queued for
 * processing, otherwise the buffer is returned to the buffer pool.
 */
public class DatagramChannelDispatcher<E extends Event<DatagramChannel>> implements ChannelDispatcher {

    private final EventFactory<E> eventFactory;
    private final BlockingQueue<ByteBuffer> bufferPool;
    private final EventQueue<E> events;
    private final ComponentLog logger;
    private final String sendingHost;
    private final Integer sendingPort;

    private Selector selector;
    private DatagramChannel datagramChannel;
    private volatile boolean stopped = false;

    public DatagramChannelDispatcher(final EventFactory<E> eventFactory,
                                     final BlockingQueue<ByteBuffer> bufferPool,
                                     final BlockingQueue<E> events,
                                     final ComponentLog logger) {
        this(eventFactory, bufferPool, events, logger, null, null);
    }

    public DatagramChannelDispatcher(final EventFactory<E> eventFactory,
                                     final BlockingQueue<ByteBuffer> bufferPool,
                                     final BlockingQueue<E> events,
                                     final ComponentLog logger,
                                     final String sendingHost,
                                     final Integer sendingPort) {
        this.eventFactory = eventFactory;
        this.bufferPool = bufferPool;
        this.logger = logger;
        this.sendingHost = sendingHost;
        this.sendingPort = sendingPort;
        this.events = new EventQueue<>(events, logger);

        if (bufferPool == null || bufferPool.size() == 0) {
            throw new IllegalArgumentException("A pool of available ByteBuffers is required");
        }
    }

    @Override
    public void open(final InetAddress nicAddress, final int port, final int maxBufferSize) throws IOException {
        stopped = false;
        datagramChannel = DatagramChannel.open();
        datagramChannel.configureBlocking(false);

        if (maxBufferSize > 0) {
            datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, maxBufferSize);
            final int actualReceiveBufSize = datagramChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            if (actualReceiveBufSize < maxBufferSize) {
                logger.warn("Attempted to set Socket Buffer Size to " + maxBufferSize + " bytes but could only set to "
                        + actualReceiveBufSize + "bytes. You may want to consider changing the Operating System's "
                        + "maximum receive buffer");
            }
        }

        // we don't have to worry about nicAddress being null here because InetSocketAddress already handles it
        datagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        datagramChannel.socket().bind(new InetSocketAddress(nicAddress, port));

        // if a sending host and port were provided then connect to that specific address to only receive
        // datagrams from that host/port, otherwise we can receive datagrams from any host/port
        if (sendingHost != null && sendingPort != null) {
            datagramChannel.connect(new InetSocketAddress(sendingHost, sendingPort));
        }

        selector = Selector.open();
        datagramChannel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public void run() {
        final ByteBuffer buffer = bufferPool.poll();
        while (!stopped) {
            try {
                int selected = selector.select();
                // if stopped the selector could already be closed which would result in a ClosedSelectorException
                if (selected > 0 && !stopped) {
                    Iterator<SelectionKey> selectorKeys = selector.selectedKeys().iterator();
                    // if stopped we don't want to modify the keys because close() may still be in progress
                    while (selectorKeys.hasNext() && !stopped) {
                        SelectionKey key = selectorKeys.next();
                        selectorKeys.remove();
                        if (!key.isValid()) {
                            continue;
                        }
                        DatagramChannel channel = (DatagramChannel) key.channel();
                        SocketAddress socketAddress;
                        buffer.clear();
                        while (!stopped && (socketAddress = channel.receive(buffer)) != null) {
                            String sender = "";
                            if (socketAddress instanceof InetSocketAddress) {
                                sender = ((InetSocketAddress) socketAddress).getAddress().toString();
                            }

                            // create a byte array from the buffer
                            buffer.flip();
                            byte bytes[] = new byte[buffer.limit()];
                            buffer.get(bytes, 0, buffer.limit());

                            final Map<String,String> metadata = EventFactoryUtil.createMapWithSender(sender);
                            final E event = eventFactory.create(bytes, metadata, null);
                            events.offer(event);

                            buffer.clear();
                        }
                    }
                }
            } catch (InterruptedException e) {
                stopped = true;
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.error("Error reading from DatagramChannel", e);
            }
        }

        if (buffer != null) {
            try {
                bufferPool.put(buffer);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public int getPort() {
        return datagramChannel == null ? 0 : datagramChannel.socket().getLocalPort();
    }

    @Override
    public void close() {
        stopped = true;
        if (selector != null) {
            selector.wakeup();
        }
        IOUtils.closeQuietly(selector);
        IOUtils.closeQuietly(datagramChannel);
    }

}
