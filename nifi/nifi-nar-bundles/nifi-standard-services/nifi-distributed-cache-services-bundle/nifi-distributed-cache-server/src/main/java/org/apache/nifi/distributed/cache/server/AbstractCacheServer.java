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
package org.apache.nifi.distributed.cache.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.net.ssl.SSLContext;

import org.apache.nifi.distributed.cache.protocol.ProtocolHandshake;
import org.apache.nifi.distributed.cache.protocol.exception.HandshakeException;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.io.socket.SocketChannelInputStream;
import org.apache.nifi.remote.io.socket.SocketChannelOutputStream;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelInputStream;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCacheServer implements CacheServer {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCacheServer.class);

    private final String identifier;
    private final int port;
    private final SSLContext sslContext;
    protected volatile boolean stopped = false;
    private final Set<Thread> processInputThreads = new CopyOnWriteArraySet<>();

    private volatile ServerSocketChannel serverSocketChannel;

    public AbstractCacheServer(final String identifier, final SSLContext sslContext, final int port) {
        this.identifier = identifier;
        this.port = port;
        this.sslContext = sslContext;
    }

    @Override
    public int getPort() {
        return serverSocketChannel == null ? this.port : serverSocketChannel.socket().getLocalPort();
    }

    @Override
    public void start() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(true);
        serverSocketChannel.bind(new InetSocketAddress(port));

        final Runnable runnable = new Runnable() {

            @Override
            public void run() {
                while (true) {
                    final SocketChannel socketChannel;
                    try {
                        socketChannel = serverSocketChannel.accept();
                        logger.debug("Connected to {}", new Object[]{socketChannel});
                    } catch (final IOException e) {
                        if (!stopped) {
                            logger.error("{} unable to accept connection from remote peer due to {}", this, e.toString());
                            if (logger.isDebugEnabled()) {
                                logger.error("", e);
                            }
                        }
                        return;
                    }

                    final Runnable processInputRunnable = new Runnable() {
                        @Override
                        public void run() {
                            final InputStream rawInputStream;
                            final OutputStream rawOutputStream;
                            final String peer = socketChannel.socket().getInetAddress().getHostName();

                            try {
                                if (sslContext == null) {
                                    rawInputStream = new SocketChannelInputStream(socketChannel);
                                    rawOutputStream = new SocketChannelOutputStream(socketChannel);
                                } else {
                                    final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslContext, socketChannel, false);
                                    sslSocketChannel.connect();
                                    rawInputStream = new SSLSocketChannelInputStream(sslSocketChannel);
                                    rawOutputStream = new SSLSocketChannelOutputStream(sslSocketChannel);
                                }
                            } catch (IOException e) {
                                logger.error("Cannot create input and/or output streams for {}", new Object[]{identifier}, e);
                                if (logger.isDebugEnabled()) {
                                    logger.error("", e);
                                }
                                try {
                                    socketChannel.close();
                                } catch (IOException swallow) {
                                }

                                return;
                            }
                            try (final InputStream in = new BufferedInputStream(rawInputStream);
                                final OutputStream out = new BufferedOutputStream(rawOutputStream)) {

                                final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(1);

                                ProtocolHandshake.receiveHandshake(in, out, versionNegotiator);

                                boolean continueComms = true;
                                while (continueComms) {
                                    continueComms = listen(in, out, versionNegotiator.getVersion());
                                }
                                // client has issued 'close'
                                logger.debug("Client issued close on {}", new Object[]{socketChannel});
                            } catch (final SocketTimeoutException e) {
                                logger.debug("30 sec timeout reached", e);
                            } catch (final IOException | HandshakeException e) {
                                if (!stopped) {
                                    logger.error("{} unable to communicate with remote peer {} due to {}", new Object[]{this, peer, e.toString()});
                                    if (logger.isDebugEnabled()) {
                                        logger.error("", e);
                                    }
                                }
                            } finally {
                                processInputThreads.remove(Thread.currentThread());
                            }
                        }
                    };

                    final Thread processInputThread = new Thread(processInputRunnable);
                    processInputThread.setName("Distributed Cache Server Communications Thread: " + identifier);
                    processInputThread.setDaemon(true);
                    processInputThread.start();
                    processInputThreads.add(processInputThread);
                }
            }
        };

        final Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("Distributed Cache Server: " + identifier);
        thread.start();
    }

    @Override
    public void stop() throws IOException {
        stopped = true;
        logger.info("Stopping CacheServer {}", new Object[]{this.identifier});

        if (serverSocketChannel != null && serverSocketChannel.isOpen()) {
            serverSocketChannel.close();
        }
        // need to close out the created SocketChannels...this is done by interrupting
        // the created threads that loop on listen().
        for (Thread processInputThread : processInputThreads) {
            processInputThread.interrupt();
            int i = 0;
            while (!processInputThread.isInterrupted() && i++ < 5) {
                try {
                    Thread.sleep(50); // allow thread to gracefully terminate
                } catch (InterruptedException e) {
                }
            }
        }
        processInputThreads.clear();
    }

    @Override
    public String toString() {
        return "CacheServer[id=" + identifier + "]";
    }

    /**
     * Listens for incoming data and communicates with remote peer
     *
     * @param in in
     * @param out out
     * @param version version
     * @return <code>true</code> if communications should continue, <code>false</code> otherwise
     * @throws IOException ex
     */
    protected abstract boolean listen(InputStream in, OutputStream out, int version) throws IOException;
}
