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
package org.apache.nifi.event.transport.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.EventSenderFactory;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.channel.pool.InitializingChannelPoolHandler;
import org.apache.nifi.event.transport.netty.channel.ssl.ClientSslStandardChannelInitializer;
import org.apache.nifi.event.transport.netty.channel.StandardChannelInitializer;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Netty Event Sender Factory
 */
public class NettyEventSenderFactory<T> extends EventLoopGroupFactory implements EventSenderFactory<T> {
    private static final int MAX_PENDING_ACQUIRES = 1024;
    private Integer socketSendBufferSize = null;

    private final String address;

    private final int port;

    private final TransportProtocol protocol;

    private Duration timeout = Duration.ofSeconds(30);

    private int maxConnections = Runtime.getRuntime().availableProcessors() * 2;

    private Supplier<List<ChannelHandler>> handlerSupplier = () -> Collections.emptyList();

    private SSLContext sslContext;

    private boolean singleEventPerConnection = false;

    public NettyEventSenderFactory(final String address, final int port, final TransportProtocol protocol) {
        this.address = address;
        this.port = port;
        this.protocol = protocol;
    }

    /**
     * Set Socket Send Buffer Size for TCP Sockets
     *
     * @param socketSendBufferSize Send Buffer size can be null to use default setting
     */
    public void setSocketSendBufferSize(final Integer socketSendBufferSize) {
        this.socketSendBufferSize = socketSendBufferSize;
    }

    /**
     * Set Channel Handler Supplier
     *
     * @param handlerSupplier Channel Handler Supplier
     */
    public void setHandlerSupplier(final Supplier<List<ChannelHandler>> handlerSupplier) {
        this.handlerSupplier = Objects.requireNonNull(handlerSupplier);
    }

    /**
     * Set SSL Context to enable TLS Channel Handler
     *
     * @param sslContext SSL Context
     */
    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    /**
     * Set Timeout for Connections and Communication
     *
     * @param timeout Timeout Duration
     */
    public void setTimeout(final Duration timeout) {
        this.timeout = Objects.requireNonNull(timeout, "Timeout required");
    }

    /**
     * Set Maximum Connections for Channel Pool
     *
     * @param maxConnections Maximum Number of connections defaults to available processors multiplied by 2
     */
    public void setMaxConnections(final int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Send a single event for the session and close the connection. Useful for endpoints which can not be configured
     * to listen for a delimiter.
     *
     * @param singleEventPerConnection true if the connection should be ended after an event is sent
     */
    public void setSingleEventPerConnection(final boolean singleEventPerConnection) {
        this.singleEventPerConnection = singleEventPerConnection;
    }

    /*
     * Get Event Sender with connected Channel
     *
     * @return Connected Event Sender
     */
    public EventSender<T> getEventSender() {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.remoteAddress(new InetSocketAddress(address, port));
        final EventLoopGroup group = getEventLoopGroup();
        bootstrap.group(group);

        if (TransportProtocol.UDP.equals(protocol)) {
            bootstrap.channel(NioDatagramChannel.class);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }

        setChannelOptions(bootstrap);
        return getConfiguredEventSender(bootstrap);
    }

    private void setChannelOptions(final Bootstrap bootstrap) {
        final int timeoutMilliseconds = (int) timeout.toMillis();
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMilliseconds);
        if (socketSendBufferSize != null) {
            bootstrap.option(ChannelOption.SO_SNDBUF, socketSendBufferSize);
        }
    }

    private EventSender<T> getConfiguredEventSender(final Bootstrap bootstrap) {
        final SocketAddress remoteAddress = bootstrap.config().remoteAddress();
        final ChannelPool channelPool = getChannelPool(bootstrap);
        return new NettyEventSender<>(bootstrap.config().group(), channelPool, remoteAddress, singleEventPerConnection);
    }

    private ChannelPool getChannelPool(final Bootstrap bootstrap) {
        final ChannelInitializer<Channel> channelInitializer = getChannelInitializer();
        final ChannelPoolHandler handler = new InitializingChannelPoolHandler(channelInitializer);
        return new FixedChannelPool(bootstrap,
                handler,
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                timeout.toMillis(),
                maxConnections,
                MAX_PENDING_ACQUIRES);
    }

    private ChannelInitializer<Channel> getChannelInitializer() {
        final StandardChannelInitializer<Channel> channelInitializer = sslContext == null
                ? new StandardChannelInitializer<>(handlerSupplier)
                : new ClientSslStandardChannelInitializer<>(handlerSupplier, sslContext);
        channelInitializer.setWriteTimeout(timeout);
        return channelInitializer;
    }
}
