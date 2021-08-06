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

import java.io.IOException;

/**
 * The implementation of the {@link DistributedCacheClient} using the netty library to provide the remote
 * communication services.
 */
public class NettyDistributedCacheClient implements DistributedCacheClient {

    /**
     * The pool of network connections used to service client requests.
     */
    private final ChannelPool channelPool;

    /**
     * Constructor.
     *
     * @param channelPool the pool of network connections used to service client requests
     */
    public NettyDistributedCacheClient(final ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    @Override
    public void invoke(final OutboundAdapter outboundAdapter, final InboundAdapter inboundAdapter) throws IOException {
        final Channel channel = channelPool.acquire().syncUninterruptibly().getNow();
        try {
            final NettyRequestHandler requestHandler = (NettyRequestHandler) channel.pipeline().last();
            final byte[] message = outboundAdapter.toBytes();
            requestHandler.invoke(channel, message, inboundAdapter);
        } finally {
            channelPool.release(channel).syncUninterruptibly();
        }
    }
}
