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
package org.apache.nifi.distributed.cache.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.nifi.event.transport.netty.channel.pool.InitializingChannelPoolHandler;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextProvider;

import javax.net.ssl.SSLContext;
import java.time.Duration;

/**
 * Factory for construction of new {@link ChannelPool}, used by distributed cache clients to invoke service methods.
 */
class CacheClientChannelPoolFactory {

    private static final int MAX_PENDING_ACQUIRES = 1024;

    private int maxConnections = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * Set Maximum Connections for Channel Pool
     *
     * @param maxConnections Maximum Number of connections defaults to available processors multiplied by 2
     */
    public void setMaxConnections(final int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * Instantiate a new netty pool of channels to be used for distributed cache communications
     *
     * @param hostname           the network name / IP address of the server running the distributed cache service
     * @param port               the port on which the distributed cache service is running
     * @param timeoutMillis      the network timeout associated with requests to the service
     * @param sslContextProvider the SSL context (if any) associated with requests to the service; if not specified,
     *                           communications will not be encrypted
     * @param factory            creator of object used to broker the version of the distributed cache protocol with the service
     * @param eventLoopGroup     Netty Event Loop Group providing threads for managing connections
     * @return a channel pool object from which {@link Channel} objects may be obtained
     */
    public ChannelPool createChannelPool(final String hostname,
                                         final int port,
                                         final int timeoutMillis,
                                         final SSLContextProvider sslContextProvider,
                                         final VersionNegotiatorFactory factory,
                                         final EventLoopGroup eventLoopGroup) {
        final SSLContext sslContext = (sslContextProvider == null) ? null : sslContextProvider.createContext();
        final Bootstrap bootstrap = new Bootstrap();
        final CacheClientChannelInitializer initializer = new CacheClientChannelInitializer(sslContext, factory, Duration.ofMillis(timeoutMillis), Duration.ofMillis(timeoutMillis));
        bootstrap.group(eventLoopGroup)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
                .remoteAddress(hostname, port)
                .channel(NioSocketChannel.class);
        final ChannelPoolHandler channelPoolHandler = new InitializingChannelPoolHandler(initializer);
        return new FixedChannelPool(bootstrap,
                channelPoolHandler,
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                timeoutMillis,
                maxConnections,
                MAX_PENDING_ACQUIRES);
    }
}
