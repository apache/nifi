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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.apache.nifi.remote.VersionNegotiator;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * Bootstrap a new netty connection.  This performs the socket handshake used by the nifi distributed set /
 * distributed map client controller services.
 */
public class NettyChannelInitializer extends ChannelInitializer<Channel> {

    /**
     * The (optional) secure context associated with the channel.  If null, channel is not SSL protected.
     */
    private final SSLContext sslContext;

    /**
     * The version of the protocol negotiated between the client and server.
     */
    private final VersionNegotiator versionNegotiator;

    /**
     * Constructor.
     *
     * @param sslContext        the secure context (if any) to be associated with the channel
     * @param versionNegotiator coordinator used to broker the version of the distributed cache protocol with the service
     */
    public NettyChannelInitializer(final SSLContext sslContext, VersionNegotiator versionNegotiator) {
        this.sslContext = sslContext;
        this.versionNegotiator = versionNegotiator;
    }

    @Override
    protected void initChannel(final Channel channel) {
        final ChannelPipeline channelPipeline = channel.pipeline();

        if (sslContext != null) {
            final SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(true);
            final SslHandler sslHandler = new SslHandler(sslEngine);
            channelPipeline.addLast(sslHandler);
        }

        channelPipeline.addLast(new NettyHandshakeHandler(channel, versionNegotiator));
        channelPipeline.addLast(new NettyRequestHandler());
    }
}
