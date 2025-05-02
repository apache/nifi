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

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.EventServerFactory;
import org.apache.nifi.event.transport.configuration.BufferAllocator;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.channel.StandardChannelInitializer;
import org.apache.nifi.event.transport.netty.channel.ssl.ServerSslHandlerChannelInitializer;
import org.apache.nifi.security.util.ClientAuth;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Netty Event Server Factory
 */
public class NettyEventServerFactory extends EventLoopGroupFactory implements EventServerFactory {
    private final InetAddress address;

    private final int port;

    private final TransportProtocol protocol;

    private Supplier<List<ChannelHandler>> handlerSupplier = Collections::emptyList;

    private Integer socketReceiveBuffer;

    private Boolean socketKeepAlive;

    private SSLContext sslContext;

    private SSLParameters sslParameters;

    private ClientAuth clientAuth = ClientAuth.NONE;

    private Duration shutdownQuietPeriod = ShutdownQuietPeriod.DEFAULT.getDuration();

    private Duration shutdownTimeout = ShutdownTimeout.DEFAULT.getDuration();

    private Duration idleTimeout = null;

    private BufferAllocator bufferAllocator = BufferAllocator.POOLED;

    public NettyEventServerFactory(final InetAddress address, final int port, final TransportProtocol protocol) {
        this.address = address;
        this.port = port;
        this.protocol = protocol;
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
     * Set Socket Keep Alive for TCP Sockets
     *
     * @param socketKeepAlive Keep Alive can be null to use default setting
     */
    public void setSocketKeepAlive(final Boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    /**
     * Set Socket Receive Buffer Size for TCP Sockets
     *
     * @param socketReceiveBuffer Receive Buffer size can be null to use default setting
     */
    public void setSocketReceiveBuffer(final Integer socketReceiveBuffer) {
        this.socketReceiveBuffer = socketReceiveBuffer;
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
     * Set SSL Parameters for optional additional configuration of TLS negotiation
     *
     * @param sslParameters SSL Parameters
     */
    public void setSslParameters(final SSLParameters sslParameters) {
        this.sslParameters = sslParameters;
    }

    /**
     * Set Client Authentication
     *
     * @param clientAuth Client Authentication
     */
    public void setClientAuth(final ClientAuth clientAuth) {
        this.clientAuth = clientAuth;
    }

    /**
     * Set shutdown quiet period
     *
     * @param quietPeriod shutdown quiet period
     */
    public void setShutdownQuietPeriod(final Duration quietPeriod) {
        this.shutdownQuietPeriod = quietPeriod;
    }

    /**
     * Set shutdown timeout
     *
     * @param timeout shutdown timeout
     */
    public void setShutdownTimeout(final Duration timeout) {
        this.shutdownTimeout = timeout;
    }

    /**
     * Set Buffer Allocator option overriding the default POOLED configuration
     *
     * @param bufferAllocator Buffer Allocator
     */
    public void setBufferAllocator(final BufferAllocator bufferAllocator) {
        this.bufferAllocator = Objects.requireNonNull(bufferAllocator, "Buffer Allocator required");
    }

    /**
     * Set an idle timeout for connections, which closes a connection when there have been no read or writes for the given timeout period.
     * Has no default - idle connections will not be closed by the server unless this timeout is set to non-zero.
     */
    public void setIdleTimeout(final Duration timeout) {
        this.idleTimeout = Objects.requireNonNull(timeout, "Timeout value required");
    }

    /**
     * Get Event Server with Channel bound to configured address and port number
     *
     * @return Event Sender
     */
    @Override
    public EventServer getEventServer() {
        final AbstractBootstrap<?, ?> bootstrap = getBootstrap();
        setChannelOptions(bootstrap);
        final EventLoopGroup group = getEventLoopGroup();
        bootstrap.group(group);
        return getBoundEventServer(bootstrap, group);
    }

    private void setChannelOptions(final AbstractBootstrap<?, ?> bootstrap) {
        if (socketReceiveBuffer != null) {
            bootstrap.option(ChannelOption.SO_RCVBUF, socketReceiveBuffer);
            bootstrap.option(ChannelOption.RECVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(socketReceiveBuffer));
        }
        if (socketKeepAlive != null) {
            bootstrap.option(ChannelOption.SO_KEEPALIVE, socketKeepAlive);
        }
        if (BufferAllocator.UNPOOLED == bufferAllocator) {
            bootstrap.option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT);
        }
    }

    private AbstractBootstrap<?, ?> getBootstrap() {
        if (TransportProtocol.UDP.equals(protocol)) {
            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.channel(NioDatagramChannel.class);
            bootstrap.handler(getChannelInitializer());
            return bootstrap;
        } else {
            final ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.channel(NioServerSocketChannel.class);
            bootstrap.childHandler(getChannelInitializer());
            return bootstrap;
        }
    }

    private ChannelInitializer<?> getChannelInitializer() {
        final StandardChannelInitializer<Channel> channelInitializer;

        if (sslContext == null) {
            channelInitializer = new StandardChannelInitializer<>(handlerSupplier);
        } else {
            final SSLParameters parameters;
            if (sslParameters == null) {
                parameters = sslContext.getDefaultSSLParameters();
            } else {
                parameters = sslParameters;
            }
            channelInitializer = new ServerSslHandlerChannelInitializer<>(handlerSupplier, sslContext, clientAuth, parameters);
        }

        if (idleTimeout != null) {
            channelInitializer.setIdleTimeout(idleTimeout);
        }
        return channelInitializer;
    }

    private EventServer getBoundEventServer(final AbstractBootstrap<?, ?> bootstrap, final EventLoopGroup group) {
        final ChannelFuture bindFuture = bootstrap.bind(address, port);
        try {
            final ChannelFuture channelFuture = bindFuture.syncUninterruptibly();
            return new NettyEventServer(group, channelFuture.channel(), shutdownQuietPeriod, shutdownTimeout);
        } catch (final Exception e) {
            group.shutdownGracefully();
            throw new EventException(String.format("Channel Bind Failed [%s:%d]", address, port), e);
        }
    }
}
