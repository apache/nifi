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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.io.nio.consumer.StreamConsumerFactory;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides the entry point to NIO based socket listeners for NiFi
 * processors and services. There are 2 supported types of Listeners, Datagram
 * (UDP based transmissions) and ServerSocket (TCP based transmissions). This
 * will create the ChannelDispatcher, which is a Runnable and is controlled via
 * the ScheduledExecutorService, which is also created by this class. The
 * ChannelDispatcher handles connections to the ServerSocketChannels and creates
 * the readers associated with the resulting SocketChannels. Additionally, this
 * creates and manages two Selectors, one for ServerSocketChannels and another
 * for SocketChannels and DatagramChannels.
 *
 * The threading model for this consists of one thread for the
 * ChannelDispatcher, one thread per added SocketChannel reader, one thread per
 * added DatagramChannel reader. The ChannelDispatcher is not scheduled with
 * fixed delay as the others are. It is throttled by the provided timeout value.
 * Within the ChannelDispatcher there are two blocking operations which will
 * block for the given timeout each time through the enclosing loop.
 *
 * All channels are cached in one of the two Selectors via their SelectionKey.
 * The serverSocketSelector maintains all the added ServerSocketChannels; the
 * socketChannelSelector maintains the all the add DatagramChannels and the
 * created SocketChannels. Further, the SelectionKey of the DatagramChannel and
 * the SocketChannel is injected with the channel's associated reader.
 *
 * All ChannelReaders will get throttled by the unavailability of buffers in the
 * provided BufferPool. This is designed to create back pressure.
 *
 */
public final class ChannelListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelListener.class);
    private final ScheduledExecutorService executor;
    private final Selector serverSocketSelector; // used to listen for new connections
    private final Selector socketChannelSelector; // used to listen on existing connections
    private final ChannelDispatcher channelDispatcher;
    private final BufferPool bufferPool;
    private final int initialBufferPoolSize;
    private volatile long channelReaderFrequencyMSecs = 50;

    public ChannelListener(final int threadPoolSize, final StreamConsumerFactory consumerFactory, final BufferPool bufferPool, int timeout,
            TimeUnit unit) throws IOException {
        this.executor = Executors.newScheduledThreadPool(threadPoolSize + 1); // need to allow for long running ChannelDispatcher thread
        this.serverSocketSelector = Selector.open();
        this.socketChannelSelector = Selector.open();
        this.bufferPool = bufferPool;
        this.initialBufferPoolSize = bufferPool.size();
        channelDispatcher = new ChannelDispatcher(serverSocketSelector, socketChannelSelector, executor, consumerFactory, bufferPool,
                timeout, unit);
        executor.schedule(channelDispatcher, 50, TimeUnit.MILLISECONDS);
    }

    public void setChannelReaderSchedulingPeriod(final long period, final TimeUnit unit) {
        channelReaderFrequencyMSecs = TimeUnit.MILLISECONDS.convert(period, unit);
        channelDispatcher.setChannelReaderFrequency(period, unit);
    }

    /**
     * Adds a server socket channel for listening to connections.
     *
     * @param nicIPAddress - if null binds to wildcard address
     * @param port - port to bind to
     * @param receiveBufferSize - size of OS receive buffer to request. If less
     * than 0 then will not be set and OS default will win.
     * @throws IOException if unable to add socket
     */
    public void addServerSocket(final InetAddress nicIPAddress, final int port, final int receiveBufferSize)
            throws IOException {
        final ServerSocketChannel ssChannel = ServerSocketChannel.open();
        ssChannel.configureBlocking(false);
        if (receiveBufferSize > 0) {
            ssChannel.setOption(StandardSocketOptions.SO_RCVBUF, receiveBufferSize);
            final int actualReceiveBufSize = ssChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            if (actualReceiveBufSize < receiveBufferSize) {
                LOGGER.warn(this + " attempted to set TCP Receive Buffer Size to "
                        + receiveBufferSize + " bytes but could only set to " + actualReceiveBufSize
                        + "bytes. You may want to consider changing the Operating System's "
                        + "maximum receive buffer");
            }
        }
        ssChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        ssChannel.bind(new InetSocketAddress(nicIPAddress, port));
        ssChannel.register(serverSocketSelector, SelectionKey.OP_ACCEPT);
    }

    /**
     * Binds to listen for data grams on the given local IPAddress/port
     *
     * @param nicIPAddress - if null will listen on wildcard address, which
     * means datagrams will be received on all local network interfaces.
     * Otherwise, will bind to the provided IP address associated with some NIC.
     * @param port - the port to listen on
     * @param receiveBufferSize - the number of bytes to request for a receive
     * buffer from OS
     * @throws IOException if unable to add channel
     */
    public void addDatagramChannel(final InetAddress nicIPAddress, final int port, final int receiveBufferSize)
            throws IOException {
        final DatagramChannel dChannel = createAndBindDatagramChannel(nicIPAddress, port, receiveBufferSize);
        dChannel.register(socketChannelSelector, SelectionKey.OP_READ);
    }

    /**
     * Binds to listen for data grams on the given local IPAddress/port and
     * restricts receipt of datagrams to those from the provided host and port,
     * must specify both. This improves performance for datagrams coming from a
     * sender that is known a-priori.
     *
     * @param nicIPAddress - if null will listen on wildcard address, which
     * means datagrams will be received on all local network interfaces.
     * Otherwise, will bind to the provided IP address associated with some NIC.
     * @param port - the port to listen on. This is used to provide a well-known
     * destination for a sender.
     * @param receiveBufferSize - the number of bytes to request for a receive
     * buffer from OS
     * @param sendingHost - the hostname, or IP address, of the sender of
     * datagrams. Only datagrams from this host will be received. If this is
     * null the wildcard ip is used, which means datagrams may be received from
     * any network interface on the local host.
     * @param sendingPort - the port used by the sender of datagrams. Only
     * datagrams from this port will be received.
     * @throws IOException if unable to add channel
     */
    public void addDatagramChannel(final InetAddress nicIPAddress, final int port, final int receiveBufferSize, final String sendingHost,
            final Integer sendingPort) throws IOException {

        if (sendingHost == null || sendingPort == null) {
            addDatagramChannel(nicIPAddress, port, receiveBufferSize);
            return;
        }
        final DatagramChannel dChannel = createAndBindDatagramChannel(nicIPAddress, port, receiveBufferSize);
        dChannel.connect(new InetSocketAddress(sendingHost, sendingPort));
        dChannel.register(socketChannelSelector, SelectionKey.OP_READ);
    }

    private DatagramChannel createAndBindDatagramChannel(final InetAddress nicIPAddress, final int port, final int receiveBufferSize)
            throws IOException {
        final DatagramChannel dChannel = DatagramChannel.open();
        dChannel.configureBlocking(false);
        if (receiveBufferSize > 0) {
            dChannel.setOption(StandardSocketOptions.SO_RCVBUF, receiveBufferSize);
            final int actualReceiveBufSize = dChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            if (actualReceiveBufSize < receiveBufferSize) {
                LOGGER.warn(this + " attempted to set UDP Receive Buffer Size to "
                        + receiveBufferSize + " bytes but could only set to " + actualReceiveBufSize
                        + "bytes. You may want to consider changing the Operating System's "
                        + "maximum receive buffer");
            }
        }
        dChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        dChannel.bind(new InetSocketAddress(nicIPAddress, port));
        return dChannel;
    }

    public void shutdown(final long period, final TimeUnit timeUnit) {
        channelDispatcher.stop();
        for (SelectionKey selectionKey : socketChannelSelector.keys()) {
            final AbstractChannelReader reader = (AbstractChannelReader) selectionKey.attachment();
            selectionKey.cancel();
            if (reader != null) {
                while (!reader.isClosed()) {
                    try {
                        Thread.sleep(channelReaderFrequencyMSecs);
                    } catch (InterruptedException e) {
                    }
                }
                final ScheduledFuture<?> readerFuture = reader.getScheduledFuture();
                readerFuture.cancel(false);
            }
            IOUtils.closeQuietly(selectionKey.channel()); // should already be closed via reader, but if reader did not exist...
        }
        IOUtils.closeQuietly(socketChannelSelector);

        for (SelectionKey selectionKey : serverSocketSelector.keys()) {
            selectionKey.cancel();
            IOUtils.closeQuietly(selectionKey.channel());
        }
        IOUtils.closeQuietly(serverSocketSelector);
        executor.shutdown();
        try {
            executor.awaitTermination(period, timeUnit);
        } catch (final InterruptedException ex) {
            LOGGER.warn("Interrupted while trying to shutdown executor");
        }
        final int currentBufferPoolSize = bufferPool.size();
        final String warning = (currentBufferPoolSize != initialBufferPoolSize) ? "Initial buffer count=" + initialBufferPoolSize
                + " Current buffer count=" + currentBufferPoolSize
                + " Could indicate a buffer leak.  Ensure all consumers are executed until they complete." : "";
        LOGGER.info("Channel listener shutdown. " + warning);
    }
}
