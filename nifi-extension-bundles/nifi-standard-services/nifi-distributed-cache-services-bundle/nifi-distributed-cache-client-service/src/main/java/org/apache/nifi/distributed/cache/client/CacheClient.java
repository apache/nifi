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

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nifi.distributed.cache.client.adapter.InboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextProvider;

import java.io.IOException;

/**
 * Encapsulate operations which may be performed using a Cache Client Service
 */
public class CacheClient {

    private static final boolean DAEMON_THREAD_ENABLED = true;

    /**
     * The pool of network connections used to service client requests.
     */
    private final ChannelPool channelPool;

    private final EventLoopGroup eventLoopGroup;

    /**
     * Constructor.
     *
     * @param hostname           the network name / IP address of the server running the distributed cache service
     * @param port               the port on which the distributed cache service is running
     * @param timeoutMillis      the network timeout associated with requests to the service
     * @param sslContextProvider the SSL context (if any) associated with requests to the service; if not specified,
     *                           communications will not be encrypted
     * @param factory            creator of object used to broker the version of the distributed cache protocol with the service
     * @param identifier         uniquely identifies this client
     */
    protected CacheClient(final String hostname,
                          final int port,
                          final int timeoutMillis,
                          final SSLContextProvider sslContextProvider,
                          final VersionNegotiatorFactory factory,
                          final String identifier) {
        final String poolName = String.format("%s[%s]", getClass().getSimpleName(), identifier);
        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(new DefaultThreadFactory(poolName, DAEMON_THREAD_ENABLED), NioIoHandler.newFactory());
        this.channelPool = new CacheClientChannelPoolFactory().createChannelPool(
                hostname, port, timeoutMillis, sslContextProvider, factory, eventLoopGroup);
    }

    /**
     * Call a service method.
     *
     * @param outboundAdapter the object used to assemble the service request byte stream
     * @param inboundAdapter  the object used to interpret the service response byte stream
     * @throws IOException on serialization failure; on communication failure
     */
    protected void invoke(final OutboundAdapter outboundAdapter, final InboundAdapter inboundAdapter) throws IOException {
        final Channel channel = channelPool.acquire().syncUninterruptibly().getNow();
        try {
            final CacheClientRequestHandler requestHandler = channel.pipeline().get(CacheClientRequestHandler.class);
            requestHandler.invoke(channel, outboundAdapter, inboundAdapter);
        } finally {
            channelPool.release(channel).syncUninterruptibly();
        }
    }

    /**
     * Close Channel Pool and supporting Event Loop Group
     */
    protected void closeChannelPool() {
        channelPool.close();
        eventLoopGroup.close();
    }
}
