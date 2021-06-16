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
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.security.util.ClientAuth;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Accepts Socket connections on the given port and creates a handler for each connection to
 * be executed by a thread pool.
 */
public class SocketChannelDispatcher<E extends Event<SocketChannel>> implements AsyncChannelDispatcher {

    private final EventFactory<E> eventFactory;
    private final ChannelHandlerFactory<E, AsyncChannelDispatcher> handlerFactory;
    private final ByteBufferSource bufferSource;
    private final BlockingQueue<E> events;
    private final ComponentLog logger;
    private final int maxConnections;
    private final int maxThreadPoolSize;
    private final SSLContext sslContext;
    private final ClientAuth clientAuth;
    private final Charset charset;

    private ThreadPoolExecutor executor;
    private volatile boolean stopped = false;
    private Selector selector;
    private final BlockingQueue<SelectionKey> keyQueue;
    private final AtomicInteger currentConnections = new AtomicInteger(0);

    public SocketChannelDispatcher(final EventFactory<E> eventFactory,
                                   final ChannelHandlerFactory<E, AsyncChannelDispatcher> handlerFactory,
                                   final ByteBufferSource bufferSource,
                                   final BlockingQueue<E> events,
                                   final ComponentLog logger,
                                   final int maxConnections,
                                   final SSLContext sslContext,
                                   final Charset charset) {
        this(eventFactory, handlerFactory, bufferSource, events, logger, maxConnections, sslContext, ClientAuth.REQUIRED, charset);
    }

    public SocketChannelDispatcher(final EventFactory<E> eventFactory,
                                   final ChannelHandlerFactory<E, AsyncChannelDispatcher> handlerFactory,
                                   final ByteBufferSource bufferSource,
                                   final BlockingQueue<E> events,
                                   final ComponentLog logger,
                                   final int maxConnections,
                                   final SSLContext sslContext,
                                   final ClientAuth clientAuth,
                                   final Charset charset) {
        this(eventFactory, handlerFactory, bufferSource, events, logger, maxConnections, maxConnections, sslContext, clientAuth, charset);
    }

    public SocketChannelDispatcher(final EventFactory<E> eventFactory,
                                   final ChannelHandlerFactory<E, AsyncChannelDispatcher> handlerFactory,
                                   final ByteBufferSource bufferSource,
                                   final BlockingQueue<E> events,
                                   final ComponentLog logger,
                                   final int maxConnections,
                                   final int maxThreadPoolSize,
                                   final SSLContext sslContext,
                                   final ClientAuth clientAuth,
                                   final Charset charset) {
        this.eventFactory = eventFactory;
        this.handlerFactory = handlerFactory;
        this.bufferSource = bufferSource;
        this.events = events;
        this.logger = logger;
        this.maxConnections = maxConnections;
        this.maxThreadPoolSize = maxThreadPoolSize;
        this.keyQueue = new LinkedBlockingQueue<>(maxConnections);
        this.sslContext = sslContext;
        this.clientAuth = clientAuth;
        this.charset = charset;
    }

    @Override
    public void open(final InetAddress nicAddress, final int port, final int maxBufferSize) throws IOException {
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(nicAddress, port);

        stopped = false;
        executor = new ThreadPoolExecutor(
                maxThreadPoolSize,
                maxThreadPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new BasicThreadFactory.Builder().namingPattern(inetSocketAddress.toString() + "-worker-%d").build());
        executor.allowCoreThreadTimeOut(true);

        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        if (maxBufferSize > 0) {
            serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, maxBufferSize);
            final int actualReceiveBufSize = serverSocketChannel.getOption(StandardSocketOptions.SO_RCVBUF);
            if (actualReceiveBufSize < maxBufferSize) {
                logger.warn("Attempted to set Socket Buffer Size to " + maxBufferSize + " bytes but could only set to "
                        + actualReceiveBufSize + "bytes. You may want to consider changing the Operating System's "
                        + "maximum receive buffer");
            }
        }

        serverSocketChannel.socket().bind(inetSocketAddress);

        selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                int selected = selector.select();
                // if stopped the selector could already be closed which would result in a ClosedSelectorException
                if (selected > 0 && !stopped){
                    Iterator<SelectionKey> selectorKeys = selector.selectedKeys().iterator();
                    // if stopped we don't want to modify the keys because close() may still be in progress
                    while (selectorKeys.hasNext() && !stopped) {
                        SelectionKey key = selectorKeys.next();
                        selectorKeys.remove();
                        if (!key.isValid()){
                            continue;
                        }
                        if (key.isAcceptable()) {
                            // Handle new connections coming in
                            final ServerSocketChannel channel = (ServerSocketChannel) key.channel();
                            final SocketChannel socketChannel = channel.accept();
                            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                            // Check for available connections
                            if (currentConnections.incrementAndGet() > maxConnections){
                                currentConnections.decrementAndGet();
                                logger.warn("Rejecting connection from {} because max connections has been met",
                                        new Object[]{ socketChannel.getRemoteAddress().toString() });
                                IOUtils.closeQuietly(socketChannel);
                                continue;
                            }
                            logger.debug("Accepted incoming connection from {}",
                                    new Object[]{socketChannel.getRemoteAddress().toString()});
                            // Set socket to non-blocking, and register with selector
                            socketChannel.configureBlocking(false);
                            SelectionKey readKey = socketChannel.register(selector, SelectionKey.OP_READ);

                            // Prepare the byte buffer for the reads, clear it out
                            ByteBuffer buffer = bufferSource.acquire();

                            // If we have an SSLContext then create an SSLEngine for the channel
                            SSLSocketChannel sslSocketChannel = null;
                            if (sslContext != null) {
                                final SSLEngine sslEngine = sslContext.createSSLEngine();
                                sslEngine.setUseClientMode(false);

                                switch (clientAuth) {
                                    case REQUIRED:
                                        sslEngine.setNeedClientAuth(true);
                                        break;
                                    case WANT:
                                        sslEngine.setWantClientAuth(true);
                                        break;
                                    case NONE:
                                        sslEngine.setNeedClientAuth(false);
                                        sslEngine.setWantClientAuth(false);
                                        break;
                                }

                                sslSocketChannel = new SSLSocketChannel(sslEngine, socketChannel);
                            }

                            // Attach the buffer and SSLSocketChannel to the key
                            SocketChannelAttachment attachment = new SocketChannelAttachment(buffer, sslSocketChannel);
                            readKey.attach(attachment);
                        } else if (key.isReadable()) {
                            // Clear out the operations the select is interested in until done reading
                            key.interestOps(0);
                            // Create a handler based on the protocol and whether an SSLEngine was provided or not
                            final Runnable handler;
                            if (sslContext != null) {
                                handler = handlerFactory.createSSLHandler(key, this, charset, eventFactory, events, logger);
                            } else {
                                handler = handlerFactory.createHandler(key, this, charset, eventFactory, events, logger);
                            }

                            // run the handler
                            executor.execute(handler);
                        }
                    }
                }
                // Add back all idle sockets to the select
                SelectionKey key;
                while((key = keyQueue.poll()) != null){
                    key.interestOps(SelectionKey.OP_READ);
                }
            } catch (IOException e) {
                logger.error("Error accepting connection from SocketChannel", e);
            }
        }
    }

    @Override
    public int getPort() {
        // Return the port for the key listening for accepts
        for(SelectionKey key : selector.keys()){
            if (key.isValid()) {
                final Channel channel = key.channel();
                if (channel instanceof  ServerSocketChannel) {
                    return ((ServerSocketChannel)channel).socket().getLocalPort();
                }
            }
        }
        return 0;
    }

    @Override
    public void close() {
        stopped = true;
        if (selector != null) {
            selector.wakeup();
        }

        if (executor != null) {
            executor.shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!executor.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                executor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }

        if (selector != null) {
            synchronized (selector.keys()) {
                for (SelectionKey key : selector.keys()) {
                    IOUtils.closeQuietly(key.channel());
                }
            }
        }
        IOUtils.closeQuietly(selector);
    }

    @Override
    public void completeConnection(SelectionKey key) {
        // connection is done. Releasing buffer
        final SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();
        bufferSource.release(attachment.getByteBuffer());
        currentConnections.decrementAndGet();
    }

    @Override
    public void addBackForSelection(SelectionKey key) {
        keyQueue.offer(key);
        selector.wakeup();
    }

}
