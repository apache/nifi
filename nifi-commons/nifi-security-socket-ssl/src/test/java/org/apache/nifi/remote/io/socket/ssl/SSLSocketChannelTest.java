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
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.File;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SSLSocketChannelTest {
    private static final String LOCALHOST = "localhost";

    private static final int GROUP_THREADS = 1;

    private static final boolean CLIENT_CHANNEL = true;

    private static final boolean SERVER_CHANNEL = false;

    private static final int CHANNEL_TIMEOUT = 15000;

    private static final int CHANNEL_FAILURE_TIMEOUT = 100;

    private static final int CHANNEL_POLL_TIMEOUT = 5000;

    private static final long CHANNEL_SLEEP_BEFORE_READ = 100;

    private static final int MAX_MESSAGE_LENGTH = 1024;

    private static final String TLS_1_3 = "TLSv1.3";

    private static final String TLS_1_2 = "TLSv1.2";

    private static final String MESSAGE = "PING\n";

    private static final Charset MESSAGE_CHARSET = StandardCharsets.UTF_8;

    private static final byte[] MESSAGE_BYTES = MESSAGE.getBytes(StandardCharsets.UTF_8);

    private static final int FIRST_BYTE_OFFSET = 1;

    private static SSLContext sslContext;

    @BeforeClass
    public static void setConfiguration() throws GeneralSecurityException, IOException {
        final TlsConfiguration tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();
        new File(tlsConfiguration.getKeystorePath()).deleteOnExit();
        new File(tlsConfiguration.getTruststorePath()).deleteOnExit();
        sslContext = SslContextFactory.createSslContext(tlsConfiguration);
    }

    @Test
    public void testClientConnectFailed() throws IOException {
        final int port = NetworkUtils.getAvailableTcpPort();
        final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslContext, LOCALHOST, port, null, CLIENT_CHANNEL);
        sslSocketChannel.setTimeout(CHANNEL_FAILURE_TIMEOUT);
        assertThrows(Exception.class, sslSocketChannel::connect);
    }

    @Test
    public void testClientConnectHandshakeFailed() throws IOException {
        assumeProtocolSupported(TLS_1_2);
        final EventLoopGroup group = new NioEventLoopGroup(GROUP_THREADS);

        try (final SocketChannel socketChannel = SocketChannel.open()) {
            final int port = NetworkUtils.getAvailableTcpPort();
            startServer(group, port, TLS_1_2);

            socketChannel.connect(new InetSocketAddress(LOCALHOST, port));
            final SSLEngine sslEngine = createSslEngine(TLS_1_2, CLIENT_CHANNEL);

            final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslEngine, socketChannel);
            sslSocketChannel.setTimeout(CHANNEL_FAILURE_TIMEOUT);

            group.shutdownGracefully().syncUninterruptibly();
            assertThrows(SSLException.class, sslSocketChannel::connect);
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    public void testClientConnectWriteReadTls12() throws Exception {
        assumeProtocolSupported(TLS_1_2);
        assertChannelConnectedWriteReadClosed(TLS_1_2);
    }

    @Test
    public void testClientConnectWriteReadTls13() throws Exception {
        assumeProtocolSupported(TLS_1_3);
        assertChannelConnectedWriteReadClosed(TLS_1_3);
    }

    @Test(timeout = CHANNEL_TIMEOUT)
    public void testServerReadWriteTls12() throws Exception {
        assumeProtocolSupported(TLS_1_2);
        assertServerChannelConnectedReadClosed(TLS_1_2);
    }

    @Test(timeout = CHANNEL_TIMEOUT)
    public void testServerReadWriteTls13() throws Exception {
        assumeProtocolSupported(TLS_1_3);
        assertServerChannelConnectedReadClosed(TLS_1_3);
    }

    private void assumeProtocolSupported(final String protocol) {
        Assume.assumeTrue(String.format("Protocol [%s] not supported", protocol), TlsPlatform.getSupportedProtocols().contains(protocol));
    }

    private void assertServerChannelConnectedReadClosed(final String enabledProtocol) throws IOException, InterruptedException {
        final int port = NetworkUtils.getAvailableTcpPort();
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        final SocketAddress socketAddress = new InetSocketAddress(LOCALHOST, port);
        serverSocketChannel.bind(socketAddress);

        final Executor executor = Executors.newSingleThreadExecutor();
        final EventLoopGroup group = new NioEventLoopGroup(GROUP_THREADS);
        try {
            final Channel channel = startClient(group, port, enabledProtocol);

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
                assertEquals("Message not matched", MESSAGE, messageRead);
            } finally {
                channel.close();
            }
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
            serverSocketChannel.close();
        }
    }

    private void assertChannelConnectedWriteReadClosed(final String enabledProtocol) throws IOException {
        processClientSslSocketChannel(enabledProtocol, (sslSocketChannel -> {
            try {
                sslSocketChannel.connect();
                assertFalse("Channel closed", sslSocketChannel.isClosed());

                assertChannelWriteRead(sslSocketChannel);

                sslSocketChannel.close();
                assertTrue("Channel not closed", sslSocketChannel.isClosed());
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Channel Failed for %s", enabledProtocol), e);
            }
        }));
    }

    private void assertChannelWriteRead(final SSLSocketChannel sslSocketChannel) throws IOException {
        sslSocketChannel.write(MESSAGE_BYTES);

        while (sslSocketChannel.available() == 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(CHANNEL_SLEEP_BEFORE_READ);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        final byte firstByteRead = (byte) sslSocketChannel.read();
        assertEquals("Channel Message first byte not matched", MESSAGE_BYTES[0], firstByteRead);

        final byte[] messageBytes = new byte[MESSAGE_BYTES.length];
        messageBytes[0] = firstByteRead;

        final int messageBytesRead = sslSocketChannel.read(messageBytes, FIRST_BYTE_OFFSET, messageBytes.length);
        assertEquals("Channel Message Bytes Read not matched", messageBytes.length - FIRST_BYTE_OFFSET, messageBytesRead);

        final String message  = new String(messageBytes, MESSAGE_CHARSET);
        assertEquals("Channel Message not matched", MESSAGE, message);
    }

    private void processClientSslSocketChannel(final String enabledProtocol, final Consumer<SSLSocketChannel> channelConsumer) throws IOException {
        final EventLoopGroup group = new NioEventLoopGroup(GROUP_THREADS);

        try {
            final int port = NetworkUtils.getAvailableTcpPort();
            startServer(group, port, enabledProtocol);
            final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslContext, LOCALHOST, port, null, CLIENT_CHANNEL);
            sslSocketChannel.setTimeout(CHANNEL_TIMEOUT);
            channelConsumer.accept(sslSocketChannel);
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
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

    private void startServer(final EventLoopGroup group, final int port, final String enabledProtocol) {
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
}
