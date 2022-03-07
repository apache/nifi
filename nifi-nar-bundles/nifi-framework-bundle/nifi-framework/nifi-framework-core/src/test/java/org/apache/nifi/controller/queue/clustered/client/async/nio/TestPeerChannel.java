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
package org.apache.nifi.controller.queue.clustered.client.async.nio;

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
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslHandler;
import org.apache.nifi.remote.io.socket.NetworkUtils;
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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPeerChannel {
    private static final String LOCALHOST = "localhost";

    private static final int GROUP_THREADS = 1;

    private static final boolean CLIENT_CHANNEL = true;

    private static final boolean SERVER_CHANNEL = false;

    private static final int CHANNEL_TIMEOUT = 15000;

    private static final int SOCKET_TIMEOUT = 5000;

    private static final int MAX_MESSAGE_LENGTH = 1024;

    private static final long SHUTDOWN_TIMEOUT = 100;

    private static final int SINGLE_COUNT_DOWN = 1;

    private static final String MESSAGE = "PING\n";

    private static final byte[] MESSAGE_BYTES = MESSAGE.getBytes(StandardCharsets.UTF_8);

    private static final String TLS_1_3 = "TLSv1.3";

    private static final String TLS_1_2 = "TLSv1.2";

    private static final String TLS_1_3_SUPPORTED = "isTls13Supported";

    private static SSLContext sslContext;

    public static boolean isTls13Supported() {
        return TlsPlatform.getSupportedProtocols().contains(TLS_1_3);
    }

    @BeforeAll
    public static void setConfiguration() throws GeneralSecurityException {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        sslContext = SslContextFactory.createSslContext(tlsConfiguration);
    }

    @Test
    @Timeout(value = CHANNEL_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    public void testConnectedClose() throws IOException {
        final String enabledProtocol = getEnabledProtocol();

        final CountDownLatch countDownLatch = getSingleCountDownLatch();
        processChannel(enabledProtocol, countDownLatch, peerChannel -> {});
    }

    @Test
    @Timeout(value = CHANNEL_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    public void testConnectedWriteReadCloseTls12() throws IOException {
        assertWriteReadSuccess(TLS_1_2);
    }

    @EnabledIf(TLS_1_3_SUPPORTED)
    @Test
    @Timeout(value = CHANNEL_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    public void testConnectedWriteReadCloseTls13() throws IOException {
        assertWriteReadSuccess(TLS_1_3);
    }

    private void assertWriteReadSuccess(final String enabledProtocol) throws IOException {
        final CountDownLatch countDownLatch = getSingleCountDownLatch();

        processChannel(enabledProtocol, countDownLatch, peerChannel -> {
            try {
                peerChannel.performHandshake();

                final ByteBuffer messageBuffer = ByteBuffer.wrap(MESSAGE_BYTES);
                final ByteBuffer encryptedBuffer = peerChannel.prepareForWrite(messageBuffer);
                peerChannel.write(encryptedBuffer);

                awaitCountDownLatch(countDownLatch);

                final OptionalInt firstRead = peerChannel.read();
                assertTrue(firstRead.isPresent(), "Peer Channel read failed");

                final int firstByteRead = firstRead.getAsInt();
                assertEquals(MESSAGE_BYTES[0], firstByteRead, "First byte read not matched");

                final int expectedBytesRemaining = MESSAGE_BYTES.length - 1;
                final ByteBuffer readBuffer = ByteBuffer.allocate(expectedBytesRemaining);
                final int bytesRead = peerChannel.read(readBuffer);
                readBuffer.flip();
                assertEquals(expectedBytesRemaining, bytesRead, "Expected bytes read not matched");

                final byte[] messageBytes = new byte[MESSAGE_BYTES.length];
                messageBytes[0] = (byte) firstByteRead;
                readBuffer.get(messageBytes, 1, readBuffer.limit());

                assertArrayEquals(MESSAGE_BYTES,messageBytes, "Message read not matched");
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Channel Failed for %s", enabledProtocol), e);
            }
        });
    }

    private void processChannel(final String enabledProtocol, final CountDownLatch countDownLatch, final Consumer<PeerChannel> channelConsumer) throws IOException {
        final EventLoopGroup group = new NioEventLoopGroup(GROUP_THREADS);

        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final Socket socket = socketChannel.socket();
            socket.setSoTimeout(SOCKET_TIMEOUT);

            final InetSocketAddress serverSocketAddress = getServerSocketAddress();
            startServer(group, serverSocketAddress.getPort(), enabledProtocol, countDownLatch);

            socketChannel.connect(serverSocketAddress);
            final SSLEngine sslEngine = createSslEngine(enabledProtocol, CLIENT_CHANNEL);

            final PeerChannel peerChannel = new PeerChannel(socketChannel, sslEngine, serverSocketAddress.toString());
            assertConnectedOpen(peerChannel);

            channelConsumer.accept(peerChannel);

            peerChannel.close();
            assertNotConnectedNotOpen(peerChannel);
        } finally {
            shutdownGroup(group);
        }
    }

    private void assertConnectedOpen(final PeerChannel peerChannel) {
        assertTrue(peerChannel.isConnected(), "Channel not connected");
        assertTrue(peerChannel.isOpen(), "Channel not open");
    }

    private void assertNotConnectedNotOpen(final PeerChannel peerChannel) {
        assertFalse(peerChannel.isConnected(), "Channel connected");
        assertFalse(peerChannel.isOpen(), "Channel open");
    }

    private void startServer(final EventLoopGroup group, final int port, final String enabledProtocol, final CountDownLatch countDownLatch) {
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

        final ChannelFuture bindFuture = bootstrap.bind(LOCALHOST, port);
        bindFuture.syncUninterruptibly();
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

    private void awaitCountDownLatch(final CountDownLatch countDownLatch) throws IOException {
        try {
            countDownLatch.await();
        } catch (final InterruptedException e) {
            throw new IOException("Count Down Interrupted", e);
        }
    }

    private void shutdownGroup(final EventLoopGroup group) {
        group.shutdownGracefully(SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS).syncUninterruptibly();
    }

    private InetSocketAddress getServerSocketAddress() {
        final int port = NetworkUtils.getAvailableTcpPort();
        return new InetSocketAddress(LOCALHOST, port);
    }

    private CountDownLatch getSingleCountDownLatch() {
        return new CountDownLatch(SINGLE_COUNT_DOWN);
    }

    private String getEnabledProtocol() {
        return isTls13Supported() ? TLS_1_3 : TLS_1_2;
    }
}
