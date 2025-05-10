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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.util.TlsPlatform;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPeerChannel {
    private static final String LOCALHOST = "localhost";

    private static final int GROUP_THREADS = 1;

    private static final boolean CLIENT_CHANNEL = true;

    private static final boolean SERVER_CHANNEL = false;

    private static final long READ_SLEEP_INTERVAL = 500;

    private static final int CHANNEL_TIMEOUT = 15000;

    private static final int SOCKET_TIMEOUT = 5000;

    private static final long SHUTDOWN_TIMEOUT = 100;

    private static final String TLS_1_3 = "TLSv1.3";

    private static final String TLS_1_2 = "TLSv1.2";

    private static final String TLS_1_3_SUPPORTED = "isTls13Supported";

    private static final int PROTOCOL_VERSION = 1;

    private static final int VERSION_ACCEPTED = 0x10;

    private static SSLContext sslContext;

    public static boolean isTls13Supported() {
        return TlsPlatform.getSupportedProtocols().contains(TLS_1_3);
    }

    @BeforeAll
    public static void setConfiguration() throws GeneralSecurityException {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        final char[] protectionParameter = new char[]{};

        sslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(protectionParameter)
                .build();
    }

    @Test
    @Timeout(value = CHANNEL_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    public void testConnectedClose() throws IOException {
        final String enabledProtocol = getEnabledProtocol();

        processChannel(enabledProtocol, peerChannel -> { });
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
        processChannel(enabledProtocol, peerChannel -> {
            try {
                peerChannel.performHandshake();

                final byte[] version = new byte[]{PROTOCOL_VERSION};
                final ByteBuffer versionBuffer = ByteBuffer.wrap(version);
                final ByteBuffer encryptedVersionBuffer = peerChannel.prepareForWrite(versionBuffer);
                peerChannel.write(encryptedVersionBuffer);

                final int firstByteRead = read(peerChannel);
                assertEquals(PROTOCOL_VERSION, firstByteRead, "Peer Channel first byte read not matched");

                final byte[] versionAccepted = new byte[]{VERSION_ACCEPTED};
                final ByteBuffer versionAcceptedBuffer = ByteBuffer.wrap(versionAccepted);
                final ByteBuffer encryptedVersionAcceptedBuffer = peerChannel.prepareForWrite(versionAcceptedBuffer);
                peerChannel.write(encryptedVersionAcceptedBuffer);

                final int secondByteRead = read(peerChannel);
                assertEquals(VERSION_ACCEPTED, secondByteRead, "Peer Channel second byte read not matched");
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Channel Failed for %s", enabledProtocol), e);
            }
        });
    }

    private int read(final PeerChannel peerChannel) throws IOException {
        OptionalInt read = peerChannel.read();
        while (read.isEmpty()) {
            try {
                TimeUnit.MILLISECONDS.sleep(READ_SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                throw new RuntimeException("Peer Channel read sleep interrupted", e);
            }
            read = peerChannel.read();
        }
        return read.getAsInt();
    }

    private void processChannel(final String enabledProtocol, final Consumer<PeerChannel> channelConsumer) throws IOException {
        final EventLoopGroup group = new MultiThreadIoEventLoopGroup(GROUP_THREADS, NioIoHandler.newFactory());

        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final Socket socket = socketChannel.socket();
            socket.setSoTimeout(SOCKET_TIMEOUT);

            final InetSocketAddress serverSocketAddress = new InetSocketAddress(LOCALHOST, 0);
            final Channel serverChannel = startServer(group, serverSocketAddress.getPort(), enabledProtocol);
            final int port = getLocalPort(serverChannel);

            final InetSocketAddress clientSocketAddress = new InetSocketAddress(LOCALHOST, port);
            socketChannel.connect(clientSocketAddress);
            final SSLEngine sslEngine = createSslEngine(enabledProtocol, CLIENT_CHANNEL);

            final PeerChannel peerChannel = new PeerChannel(socketChannel, sslEngine, clientSocketAddress.toString());
            assertConnectedOpen(peerChannel);

            socketChannel.configureBlocking(false);
            channelConsumer.accept(peerChannel);

            peerChannel.close();
            assertNotConnectedNotOpen(peerChannel);
        } finally {
            shutdownGroup(group);
        }
    }

    private int getLocalPort(final Channel serverChannel) {
        final SocketAddress address = serverChannel.localAddress();
        if (address instanceof InetSocketAddress) {
            return ((InetSocketAddress) address).getPort();
        }
        return 0;
    }

    private void assertConnectedOpen(final PeerChannel peerChannel) {
        assertTrue(peerChannel.isConnected(), "Channel not connected");
        assertTrue(peerChannel.isOpen(), "Channel not open");
    }

    private void assertNotConnectedNotOpen(final PeerChannel peerChannel) {
        assertFalse(peerChannel.isConnected(), "Channel connected");
        assertFalse(peerChannel.isOpen(), "Channel open");
    }

    private Channel startServer(final EventLoopGroup group, final int port, final String enabledProtocol) {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(group);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(final Channel channel) {
                final ChannelPipeline pipeline = channel.pipeline();
                final SSLEngine sslEngine = createSslEngine(enabledProtocol, SERVER_CHANNEL);
                setPipelineHandlers(pipeline, sslEngine);
                pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                    private int protocolVersion;

                    @Override
                    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
                        if (byteBuf.readableBytes() == 1) {
                            final int read = byteBuf.readByte();
                            if (PROTOCOL_VERSION == read) {
                                protocolVersion = read;
                                channelHandlerContext.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{PROTOCOL_VERSION}));
                            } else if (protocolVersion == PROTOCOL_VERSION) {
                                channelHandlerContext.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{VERSION_ACCEPTED}));
                            } else {
                                throw new SocketException(String.format("Unexpected Integer [%d] read", read));
                            }
                        }
                    }
                });
            }
        });

        final ChannelFuture bindFuture = bootstrap.bind(LOCALHOST, port);
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
    }

    private void shutdownGroup(final EventLoopGroup group) {
        group.shutdownGracefully(SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS).syncUninterruptibly();
    }

    private String getEnabledProtocol() {
        return isTls13Supported() ? TLS_1_3 : TLS_1_2;
    }
}
