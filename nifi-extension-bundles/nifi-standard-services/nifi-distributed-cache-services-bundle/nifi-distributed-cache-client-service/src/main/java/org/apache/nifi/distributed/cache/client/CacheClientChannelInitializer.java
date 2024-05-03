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
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.apache.nifi.event.transport.netty.CloseContextIdleStateHandler;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.VersionNegotiatorFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Bootstrap a new netty connection.  This performs the socket handshake used by the nifi distributed set /
 * distributed map client controller services.
 */
public class CacheClientChannelInitializer extends ChannelInitializer<Channel> {

    /**
     * The (optional) secure context associated with the channel.  If null, channel is not SSL protected.
     */
    private final SSLContext sslContext;

    /**
     * The factory used to create the {@link org.apache.nifi.remote.VersionNegotiator}
     * upon initialization of the channel.
     */
    private final VersionNegotiatorFactory versionNegotiatorFactory;

    private final Duration idleTimeout;
    private final Duration writeTimeout;

    /**
     * Constructor.
     *
     * @param sslContext the secure context (if any) to be associated with the channel
     * @param factory    creator of object used to broker the version of the distributed cache protocol with the service
     */
    public CacheClientChannelInitializer(final SSLContext sslContext, final VersionNegotiatorFactory factory, final Duration idleTimeout, final Duration writeTimeout) {
        this.sslContext = sslContext;
        this.versionNegotiatorFactory = factory;
        this.idleTimeout = idleTimeout;
        this.writeTimeout = writeTimeout;
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

        final VersionNegotiator versionNegotiator = versionNegotiatorFactory.create();
        channelPipeline.addFirst(new IdleStateHandler(idleTimeout.getSeconds(), idleTimeout.getSeconds(), idleTimeout.getSeconds(), TimeUnit.SECONDS));
        channelPipeline.addLast(new WriteTimeoutHandler(writeTimeout.toMillis(), TimeUnit.MILLISECONDS));
        channelPipeline.addLast(new CacheClientHandshakeHandler(channel, versionNegotiator, writeTimeout.toMillis()));
        channelPipeline.addLast(new CacheClientRequestHandler(writeTimeout.toMillis()));
        channelPipeline.addLast(new CloseContextIdleStateHandler());
    }
}
