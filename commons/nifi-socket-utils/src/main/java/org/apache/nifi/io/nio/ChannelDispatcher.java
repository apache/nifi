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
package org.apache.nifi.io.nio;

import java.io.IOException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.io.nio.consumer.StreamConsumerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author none
 */
public final class ChannelDispatcher implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelDispatcher.class);
    private final Selector serverSocketSelector;
    private final Selector socketChannelSelector;
    private final ScheduledExecutorService executor;
    private final BufferPool emptyBuffers;
    private final StreamConsumerFactory factory;
    private final AtomicLong channelReaderFrequencyMilliseconds = new AtomicLong(DEFAULT_CHANNEL_READER_PERIOD_MILLISECONDS);
    private final long timeout;
    private volatile boolean stop = false;
    public static final long DEFAULT_CHANNEL_READER_PERIOD_MILLISECONDS = 100L;

    public ChannelDispatcher(final Selector serverSocketSelector, final Selector socketChannelSelector, final ScheduledExecutorService service,
            final StreamConsumerFactory factory, final BufferPool buffers, final long timeout, final TimeUnit unit) {
        this.serverSocketSelector = serverSocketSelector;
        this.socketChannelSelector = socketChannelSelector;
        this.executor = service;
        this.factory = factory;
        emptyBuffers = buffers;
        this.timeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
    }

    public void setChannelReaderFrequency(final long period, final TimeUnit timeUnit) {
        channelReaderFrequencyMilliseconds.set(TimeUnit.MILLISECONDS.convert(period, timeUnit));
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                selectServerSocketKeys();
                selectSocketChannelKeys();
            } catch (final Exception ex) {
                LOGGER.warn("Key selection failed: {} Normal during shutdown.", new Object[]{ex});
            }
        }
    }

    /*
     * When serverSocketsChannels are registered with the selector, want each invoke of this method to loop through all
     * channels' keys.
     * 
     * @throws IOException
     */
    private void selectServerSocketKeys() throws IOException {
        int numSelected = serverSocketSelector.select(timeout);
        if (numSelected == 0) {
            return;
        }

        // for each registered server socket - see if any connections are waiting to be established
        final Iterator<SelectionKey> itr = serverSocketSelector.selectedKeys().iterator();
        while (itr.hasNext()) {
            SelectionKey serverSocketkey = itr.next();
            final SelectableChannel channel = serverSocketkey.channel();
            AbstractChannelReader reader = null;
            if (serverSocketkey.isValid() && serverSocketkey.isAcceptable()) {
                final ServerSocketChannel ssChannel = (ServerSocketChannel) serverSocketkey.channel();
                final SocketChannel sChannel = ssChannel.accept();
                if (sChannel != null) {
                    sChannel.configureBlocking(false);
                    final SelectionKey socketChannelKey = sChannel.register(socketChannelSelector, SelectionKey.OP_READ);
                    final String readerId = sChannel.socket().toString();
                    reader = new SocketChannelReader(readerId, socketChannelKey, emptyBuffers, factory);
                    final ScheduledFuture<?> readerFuture = executor.scheduleWithFixedDelay(reader, 10L,
                            channelReaderFrequencyMilliseconds.get(), TimeUnit.MILLISECONDS);
                    reader.setScheduledFuture(readerFuture);
                    socketChannelKey.attach(reader);
                }
            }
            itr.remove(); // do this so that the next select operation returns a positive value; otherwise, it will return 0.
            if (reader != null && LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " New Connection established.  Server channel: " + channel + " Reader: " + reader);
            }
        }
    }

    /*
     * When invoking this method, only want to iterate through the selected keys once. When a key is entered into the selectors
     * selected key set, select will return a positive value. The next select will return 0 if nothing has changed. Note that
     * the selected key set is not manually changed via a remove operation.
     * 
     * @throws IOException
     */
    private void selectSocketChannelKeys() throws IOException {
        // once a channel associated with a key in this selector is 'ready', it causes this select to immediately return.
        // thus, for each trip through the run() we only get hit with one real timeout...the one in selectServerSocketKeys.
        int numSelected = socketChannelSelector.select(timeout);
        if (numSelected == 0) {
            return;
        }

        for (SelectionKey socketChannelKey : socketChannelSelector.selectedKeys()) {
            final SelectableChannel channel = socketChannelKey.channel();
            AbstractChannelReader reader = null;
            // there are 2 kinds of channels in this selector, both which have their own readers and are executed in their own
            // threads. We will get here whenever a new SocketChannel is created due to an incoming connection. However,
            // for a DatagramChannel we don't want to create a new reader unless it is a new DatagramChannel. The only
            // way to tell if it's new is the lack of an attachment. 
            if (channel instanceof DatagramChannel && socketChannelKey.attachment() == null) {
                reader = new DatagramChannelReader(UUID.randomUUID().toString(), socketChannelKey, emptyBuffers, factory);
                socketChannelKey.attach(reader);
                final ScheduledFuture<?> readerFuture = executor.scheduleWithFixedDelay(reader, 10L, channelReaderFrequencyMilliseconds.get(),
                        TimeUnit.MILLISECONDS);
                reader.setScheduledFuture(readerFuture);
            }
            if (reader != null && LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " New Connection established.  Server channel: " + channel + " Reader: " + reader);
            }
        }

    }

    public void stop() {
        stop = true;
    }

}
