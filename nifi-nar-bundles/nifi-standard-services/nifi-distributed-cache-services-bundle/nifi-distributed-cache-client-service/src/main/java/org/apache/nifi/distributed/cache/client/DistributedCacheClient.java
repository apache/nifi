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
import io.netty.channel.pool.ChannelPool;
import org.apache.nifi.distributed.cache.client.adapter.InboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextService;

import java.io.IOException;

/**
 * Encapsulate operations which may be performed using a {@link DistributedSetCacheClientService} or a
 * {@link DistributedMapCacheClientService}.
 */
public class DistributedCacheClient {

    /**
     * The pool of network connections used to service client requests.
     */
    private final ChannelPool channelPool;

    /**
     * Constructor.
     *
     * @param hostname          the network name / IP address of the server running the distributed cache service
     * @param port              the port on which the distributed cache service is running
     * @param timeoutMillis     the network timeout associated with requests to the service
     * @param sslContextService the SSL context (if any) associated with requests to the service; if not specified,
     *                          communications will not be encrypted
     * @param factory           creator of object used to broker the version of the distributed cache protocol with the service
     */
    protected DistributedCacheClient(final String hostname,
                                     final int port,
                                     final int timeoutMillis,
                                     final SSLContextService sslContextService,
                                     final VersionNegotiatorFactory factory) {
        this.channelPool = new CacheClientChannelPoolFactory().createChannelPool(
                hostname, port, timeoutMillis, sslContextService, factory);
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
            final CacheClientRequestHandler requestHandler = (CacheClientRequestHandler) channel.pipeline().last();
            requestHandler.invoke(channel, outboundAdapter, inboundAdapter);
        } finally {
            channelPool.release(channel).syncUninterruptibly();
        }
    }

    /**
     * Shutdown {@link ChannelPool} cleanly.
     */
    protected void closeChannelPool() {
        channelPool.close();
    }
}
