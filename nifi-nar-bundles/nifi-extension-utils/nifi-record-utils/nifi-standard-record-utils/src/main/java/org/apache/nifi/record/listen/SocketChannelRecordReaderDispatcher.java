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
package org.apache.nifi.record.listen;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.serialization.RecordReaderFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Accepts connections on the given ServerSocketChannel and dispatches a SocketChannelRecordReader for processing.
 */
public class SocketChannelRecordReaderDispatcher implements Runnable, Closeable {

    private final ServerSocketChannel serverSocketChannel;
    private final SSLContext sslContext;
    private final SslContextFactory.ClientAuth clientAuth;
    private final int socketReadTimeout;
    private final int receiveBufferSize;
    private final int maxConnections;
    private final RecordReaderFactory readerFactory;
    private final BlockingQueue<SocketChannelRecordReader> recordReaders;
    private final ComponentLog logger;

    private final AtomicInteger currentConnections = new AtomicInteger(0);

    private volatile boolean stopped = false;

    public SocketChannelRecordReaderDispatcher(final ServerSocketChannel serverSocketChannel,
                                               final SSLContext sslContext,
                                               final SslContextFactory.ClientAuth clientAuth,
                                               final int socketReadTimeout,
                                               final int receiveBufferSize,
                                               final int maxConnections,
                                               final RecordReaderFactory readerFactory,
                                               final BlockingQueue<SocketChannelRecordReader> recordReaders,
                                               final ComponentLog logger) {
        this.serverSocketChannel = serverSocketChannel;
        this.sslContext = sslContext;
        this.clientAuth = clientAuth;
        this.socketReadTimeout = socketReadTimeout;
        this.receiveBufferSize = receiveBufferSize;
        this.maxConnections = maxConnections;
        this.readerFactory = readerFactory;
        this.recordReaders = recordReaders;
        this.logger = logger;
    }

    @Override
    public void run() {
        while(!stopped) {
            try {
                final SocketChannel socketChannel = serverSocketChannel.accept();
                if (socketChannel == null) {
                    Thread.sleep(20);
                    continue;
                }

                final SocketAddress remoteSocketAddress = socketChannel.getRemoteAddress();
                socketChannel.socket().setSoTimeout(socketReadTimeout);
                socketChannel.socket().setReceiveBufferSize(receiveBufferSize);

                if (currentConnections.incrementAndGet() > maxConnections){
                    currentConnections.decrementAndGet();
                    final String remoteAddress = remoteSocketAddress == null ? "null" : remoteSocketAddress.toString();
                    logger.warn("Rejecting connection from {} because max connections has been met", new Object[]{remoteAddress});
                    IOUtils.closeQuietly(socketChannel);
                    continue;
                }

                if (logger.isDebugEnabled()) {
                    final String remoteAddress = remoteSocketAddress == null ? "null" : remoteSocketAddress.toString();
                    logger.debug("Accepted connection from {}", new Object[]{remoteAddress});
                }

                // create a StandardSocketChannelRecordReader or an SSLSocketChannelRecordReader based on presence of SSLContext
                final SocketChannelRecordReader socketChannelRecordReader;
                if (sslContext == null) {
                    socketChannelRecordReader = new StandardSocketChannelRecordReader(socketChannel, readerFactory, this);
                } else {
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

                    final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslEngine, socketChannel);
                    socketChannelRecordReader = new SSLSocketChannelRecordReader(socketChannel, sslSocketChannel, readerFactory, this);
                }

                // queue the SocketChannelRecordReader for processing by the processor
                recordReaders.offer(socketChannelRecordReader);

            } catch (Exception e) {
                logger.error("Error dispatching connection: " + e.getMessage(), e);
            }
        }
    }

    public int getPort() {
        return serverSocketChannel == null ? 0 : serverSocketChannel.socket().getLocalPort();
    }

    @Override
    public void close() {
        this.stopped = true;
        IOUtils.closeQuietly(this.serverSocketChannel);
    }

    public void connectionCompleted() {
        currentConnections.decrementAndGet();
    }

}
