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
package org.apache.nifi.processors.standard.ssh.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.sshd.common.AttributeRepository;
import org.apache.sshd.common.FactoryManager;
import org.apache.sshd.common.future.CancelFuture;
import org.apache.sshd.common.io.DefaultIoConnectFuture;
import org.apache.sshd.common.io.IoConnectFuture;
import org.apache.sshd.common.io.IoHandler;
import org.apache.sshd.core.CoreModuleProperties;
import org.apache.sshd.netty.NettyIoConnector;
import org.apache.sshd.netty.NettyIoServiceFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Standard extension of Netty IO Connector supporting Proxy Configuration
 */
public class StandardNettyIoConnector extends NettyIoConnector {
    private final EventLoopGroup eventLoopGroup;

    private final ProxyConfiguration proxyConfiguration;

    private final Duration socketTimeout;

    private final FactoryManager factoryManager;

    public StandardNettyIoConnector(
            final NettyIoServiceFactory factory,
            final IoHandler handler,
            final EventLoopGroup eventLoopGroup,
            final ProxyConfiguration proxyConfiguration,
            final Duration socketTimeout,
            final FactoryManager factoryManager
    ) {
        super(factory, handler);
        this.eventLoopGroup = eventLoopGroup;
        this.proxyConfiguration = proxyConfiguration;
        this.socketTimeout = socketTimeout;
        this.factoryManager = factoryManager;
    }

    @Override
    public IoConnectFuture connect(final SocketAddress address, final AttributeRepository context, final SocketAddress localAddress) {
        final IoConnectFuture connectFuture = new DefaultIoConnectFuture(address, null);

        final Bootstrap bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .attr(CONNECT_FUTURE_KEY, connectFuture)
                .handler(new StandardChannelInitializer());

        // Set Socket Connect Timeout using SSHD Module Properties
        final Duration connectTimeout = CoreModuleProperties.IO_CONNECT_TIMEOUT.getRequired(factoryManager);
        final int connectTimeoutMillis = Math.toIntExact(connectTimeout.toMillis());
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);

        // Start connection
        final ChannelFuture channelFuture;
        if (localAddress == null) {
            channelFuture = bootstrap.connect(address);
        } else {
            channelFuture = bootstrap.connect(address, localAddress);
        }

        // Cancel Channel Future on Connect Future cancellation
        connectFuture.addListener(f -> {
            if (f.isCanceled()) {
                if (channelFuture.cancel(true) || channelFuture.isCancelled()) {
                    f.getCancellation().setCanceled();
                }
            }
        });

        // Add Listener for exception handling
        channelFuture.addListener(cf -> {
            final Throwable t = cf.cause();
            if (t != null) {
                connectFuture.setException(t);
            } else if (cf.isCancelled()) {
                final CancelFuture cancellation = connectFuture.cancel();
                if (cancellation != null) {
                    cancellation.setCanceled();
                }
            }
        });

        return connectFuture;
    }

    private class StandardChannelInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(final SocketChannel socketChannel) {
            final StandardNettyIoSession session = new StandardNettyIoSession(StandardNettyIoConnector.this, handler, null);
            final ChannelPipeline channelPipeline = socketChannel.pipeline();

            // Add Read Timeout Handler to close channel after failure to receive data within configured interval
            channelPipeline.addLast(new ReadTimeoutHandler(socketTimeout.toMillis(), TimeUnit.MILLISECONDS));

            // Add Proxy Handler when configured before other Handlers
            final Proxy.Type proxyType = proxyConfiguration.getProxyType();
            if (Proxy.Type.SOCKS == proxyType || Proxy.Type.HTTP == proxyType) {
                final ProxyHandler proxyHandler = getProxyHandler();
                channelPipeline.addLast(proxyHandler);
            }

            channelPipeline.addLast(session.getAdapter());
        }
    }

    private ProxyHandler getProxyHandler() {
        final String proxyServerHost = proxyConfiguration.getProxyServerHost();
        final int proxyServerPort = proxyConfiguration.getProxyServerPort();
        final InetSocketAddress proxyAddress = new InetSocketAddress(proxyServerHost, proxyServerPort);

        final String proxyUsername = proxyConfiguration.getProxyUserName();
        final String proxyPassword = proxyConfiguration.getProxyUserPassword();

        final ProxyHandler proxyHandler;
        final Proxy.Type proxyType = proxyConfiguration.getProxyType();
        if (Proxy.Type.SOCKS == proxyType) {
            if (proxyPassword == null) {
                proxyHandler = new Socks5ProxyHandler(proxyAddress);
            } else {
                proxyHandler = new Socks5ProxyHandler(proxyAddress, proxyUsername, proxyPassword);
            }
        } else if (Proxy.Type.HTTP == proxyType) {
            if (proxyPassword == null) {
                proxyHandler = new HttpProxyHandler(proxyAddress);
            } else {
                proxyHandler = new HttpProxyHandler(proxyAddress, proxyUsername, proxyPassword);
            }
        } else {
            throw new IllegalArgumentException("Proxy Type [%s] not supported".formatted(proxyType));
        }

        return proxyHandler;
    }
}
