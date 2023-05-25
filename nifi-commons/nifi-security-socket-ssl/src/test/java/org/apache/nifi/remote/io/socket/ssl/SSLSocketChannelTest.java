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
package org.apache.nifi.remote.io.socket.ssl;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslHandler;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 15)
public class SSLSocketChannelTest {
    private static final String LOCALHOST = "localhost";

    private static final int GROUP_THREADS = 1;

    private static final boolean CLIENT_CHANNEL = true;

    private static final boolean SERVER_CHANNEL = false;

    private static final int CHANNEL_TIMEOUT = 15000;

    private static final int CHANNEL_FAILURE_TIMEOUT = 100;

    private static final int CHANNEL_POLL_TIMEOUT = 5000;

    private static final int MAX_MESSAGE_LENGTH = 1024;

    private static final long SHUTDOWN_TIMEOUT = 100;

    private static final String TLS_1_3 = "TLSv1.3";

    private static final String TLS_1_2 = "TLSv1.2";

    private static final String MESSAGE = "PING\n";

    private static final Charset MESSAGE_CHARSET = StandardCharsets.UTF_8;

    private static final byte[] MESSAGE_BYTES = MESSAGE.getBytes(StandardCharsets.UTF_8);

    private static final int FIRST_BYTE_OFFSET = 1;

    private static final int SINGLE_COUNT_DOWN = 1;

    private static SSLContext sslContext;

    private static final String TLS_1_3_SUPPORTED = "isTls13Supported";

    public static boolean isTls13Supported() {
        return TlsPlatform.getSupportedProtocols().contains(TLS_1_3);
    }

    @BeforeAll
    public static void setConfiguration() throws GeneralSecurityException {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        sslContext = SslContextFactory.createSslContext(tlsConfiguration);
    }

    @Test
    public void testClientConnectFailed() throws IOException {
        final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslContext, "this-host-does-not-exist", 1, null, CLIENT_CHANNEL);
        sslSocketChannel.setTimeout(CHANNEL_FAILURE_TIMEOUT);
        assertThrows(Exception.class, sslSocketChannel::connect);
    }

    @Test
    public void testClientConnectHandshakeFailed() throws IOException {
        final String enabledProtocol = isTls13Supported() ? TLS_1_3 : TLS_1_2;

        final EventLoopGroup group = new NioEventLoopGroup(GROUP_THREADS);

        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final Channel serverChannel = startServer(group, enabledProtocol, getSingleCountDownLatch());
            final int port = getListeningPort(serverChannel);

            socketChannel.connect(new InetSocketAddress(LOCALHOST, port));
            final SSLEngine sslEngine = createSslEngine(enabledProtocol, CLIENT_CHANNEL);

            final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslEngine, socketChannel);
            sslSocketChannel.setTimeout(CHANNEL_FAILURE_TIMEOUT);

            shutdownGroup(group);
            assertThrows(SSLException.class, sslSocketChannel::connect);
        } finally {
            shutdownGroup(group);
        }
    }

    @Test
    public void testClientConnectWriteReadTls12() throws Exception {
        assertChannelConnectedWriteReadClosed(TLS_1_2);
    }

    @EnabledIf(TLS_1_3_SUPPORTED)
    @Test
    public void testClientConnectWriteReadTls13() throws Exception {
        assertChannelConnectedWriteReadClosed(TLS_1_3);
    }

    @Test
    public void testClientConnectWriteAvailableReadTls12() throws Exception {
        assertChannelConnectedWriteAvailableRead(TLS_1_2);
    }

    @EnabledIf(TLS_1_3_SUPPORTED)
    @Test
    public void testClientConnectWriteAvailableReadTls13() throws Exception {
        assertChannelConnectedWriteAvailableRead(TLS_1_3);
    }

    @Test
    @Timeout(value = CHANNEL_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    public void testServerReadWriteTls12() throws Exception {
        assertServerChannelConnectedReadClosed(TLS_1_2);
    }

    @EnabledIf(TLS_1_3_SUPPORTED)
    @Test
    @Timeout(value = CHANNEL_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    public void testServerReadWriteTls13() throws Exception {
        assertServerChannelConnectedReadClosed(TLS_1_3);
    }

    private void assertServerChannelConnectedReadClosed(final String enabledProtocol) throws IOException, InterruptedException {
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        final SocketAddress socketAddress = new InetSocketAddress(LOCALHOST, 0);
        serverSocketChannel.bind(socketAddress);

        final Executor executor = Executors.newSingleThreadExecutor();
        final EventLoopGroup group = new NioEventLoopGroup(GROUP_THREADS);
        try {
            final SocketAddress serverLocalAddress = serverSocketChannel.getLocalAddress();
            final int listeningPort = (serverLocalAddress instanceof InetSocketAddress) ? ((InetSocketAddress) serverLocalAddress).getPort() : 0;

            final Channel channel = startClient(group, listeningPort, enabledProtocol);

            try {
                final SocketChannel socketChannel = serverSocketChannel.accept();
                final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslContext, socketChannel, SERVER_CHANNEL);

                final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
                final Runnable readCommand = () -> {
                    final byte[] messageBytes = new byte[MESSAGE_BYTES.length];
                    try {
                        final int messageBytesRead = sslSocketChannel.read(messageBytes);
                        if (messageBytesRead == MESSAGE_BYTES.length) {
                            queue.add(new String(messageBytes, MESSAGE_CHARSET));
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };
                executor.execute(readCommand);
                channel.writeAndFlush(MESSAGE).syncUninterruptibly();

                final String messageRead = queue.poll(CHANNEL_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                assertEquals(MESSAGE, messageRead, "Message not matched");
            } finally {
                channel.close();
            }
        } finally {
            shutdownGroup(group);
            serverSocketChannel.close();
        }
    }

    private void assertChannelConnectedWriteReadClosed(final String enabledProtocol) throws IOException {
        final CountDownLatch countDownLatch = getSingleCountDownLatch();
        processClientSslSocketChannel(enabledProtocol, countDownLatch, (sslSocketChannel -> {
            try {
                sslSocketChannel.connect();
                assertFalse(sslSocketChannel.isClosed());

                assertChannelWriteRead(sslSocketChannel, countDownLatch);

                sslSocketChannel.close();
                assertTrue(sslSocketChannel.isClosed());
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Channel Failed for %s", enabledProtocol), e);
            }
        }));
    }

    private void assertChannelConnectedWriteAvailableRead(final String enabledProtocol) throws IOException {
        final CountDownLatch countDownLatch = getSingleCountDownLatch();
        processClientSslSocketChannel(enabledProtocol, countDownLatch, (sslSocketChannel -> {
            try {
                sslSocketChannel.connect();
                assertFalse(sslSocketChannel.isClosed());

                assertChannelWriteAvailableRead(sslSocketChannel, countDownLatch);

                sslSocketChannel.close();
                assertTrue(sslSocketChannel.isClosed());
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Channel Failed for %s", enabledProtocol), e);
            }
        }));
    }

    private void assertChannelWriteAvailableRead(final SSLSocketChannel sslSocketChannel, final CountDownLatch countDownLatch) throws IOException {
        sslSocketChannel.write(MESSAGE_BYTES);
        sslSocketChannel.available();
        awaitCountDownLatch(countDownLatch);
        assetMessageRead(sslSocketChannel);
    }

    private void assertChannelWriteRead(final SSLSocketChannel sslSocketChannel, final CountDownLatch countDownLatch) throws IOException {
        sslSocketChannel.write(MESSAGE_BYTES);
        awaitCountDownLatch(countDownLatch);
        assetMessageRead(sslSocketChannel);
    }

    private void awaitCountDownLatch(final CountDownLatch countDownLatch) throws IOException {
        try {
            countDownLatch.await();
        } catch (final InterruptedException e) {
            throw new IOException("Count Down Interrupted", e);
        }
    }

    private void assetMessageRead(final SSLSocketChannel sslSocketChannel) throws IOException {
        final byte firstByteRead = (byte) sslSocketChannel.read();
        assertEquals(MESSAGE_BYTES[0], firstByteRead, "Channel Message first byte not matched");

        final int available = sslSocketChannel.available();
        final int availableExpected = MESSAGE_BYTES.length - FIRST_BYTE_OFFSET;
        assertEquals(availableExpected, available, "Available Bytes not matched");

        final byte[] messageBytes = new byte[MESSAGE_BYTES.length];
        messageBytes[0] = firstByteRead;

        final int messageBytesRead = sslSocketChannel.read(messageBytes, FIRST_BYTE_OFFSET, messageBytes.length);
        assertEquals(messageBytes.length - FIRST_BYTE_OFFSET, messageBytesRead, "Channel Message Bytes Read not matched");

        final String message  = new String(messageBytes, MESSAGE_CHARSET);
        assertEquals(MESSAGE, message, "Message not matched");
    }

    private void processClientSslSocketChannel(final String enabledProtocol, final CountDownLatch countDownLatch, final Consumer<SSLSocketChannel> channelConsumer) throws IOException {
        final EventLoopGroup group = new NioEventLoopGroup(GROUP_THREADS);

        try {
            final Channel channel = startServer(group, enabledProtocol, countDownLatch);
            final int port = getListeningPort(channel);
            final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslContext, LOCALHOST, port, null, CLIENT_CHANNEL);
            sslSocketChannel.setTimeout(CHANNEL_TIMEOUT);
            channelConsumer.accept(sslSocketChannel);
        } finally {
            shutdownGroup(group);
        }
    }

    private int getListeningPort(final Channel serverChannel) {
        final SocketAddress address = serverChannel.localAddress();
        if (address instanceof InetSocketAddress) {
            return ((InetSocketAddress) address).getPort();
        }

        return 0;
    }

    private Channel startClient(final EventLoopGroup group, final int port, final String enabledProtocol) {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(final Channel channel) {
                final ChannelPipeline pipeline = channel.pipeline();
                final SSLEngine sslEngine = createSslEngine(enabledProtocol, CLIENT_CHANNEL);
                setPipelineHandlers(pipeline, sslEngine);
            }
        });
        return bootstrap.connect(LOCALHOST, port).syncUninterruptibly().channel();
    }

    private Channel startServer(final EventLoopGroup group, final String enabledProtocol, final CountDownLatch countDownLatch) {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(group);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(final Channel channel) {
                final ChannelPipeline pipeline = channel.pipeline();
                final SSLEngine sslEngine = createSslEngine(enabledProtocol, SERVER_CHANNEL);
                setPipelineHandlers(pipeline, sslEngine);
                pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext channelHandlerContext, String s) throws Exception {
                        channelHandlerContext.channel().writeAndFlush(MESSAGE).sync();
                        countDownLatch.countDown();
                    }
                });
            }
        });

        final ChannelFuture bindFuture = bootstrap.bind(LOCALHOST, 0);
        bindFuture.syncUninterruptibly();
        return bindFuture.channel();
    }

    private SSLEngine createSslEngine(final String enabledProtocol, final boolean useClientMode) {
        final SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(useClientMode);
        sslEngine.setEnabledProtocols(new String[]{enabledProtocol});
        return sslEngine;
    }

    private void setPipelineHandlers(final ChannelPipeline pipeline, final SSLEngine sslEngine) {
        pipeline.addLast(new SslHandler(sslEngine));
        pipeline.addLast(new DelimiterBasedFrameDecoder(MAX_MESSAGE_LENGTH, Delimiters.lineDelimiter()));
        pipeline.addLast(new StringDecoder());
        pipeline.addLast(new StringEncoder());
    }

    private void shutdownGroup(final EventLoopGroup group) {
        group.shutdownGracefully(SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS).syncUninterruptibly();
    }

    private CountDownLatch getSingleCountDownLatch() {
        return new CountDownLatch(SINGLE_COUNT_DOWN);
    }
}
