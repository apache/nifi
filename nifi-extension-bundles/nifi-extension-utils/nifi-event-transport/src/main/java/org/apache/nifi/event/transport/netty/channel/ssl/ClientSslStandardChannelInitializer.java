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
package org.apache.nifi.event.transport.netty.channel.ssl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.apache.nifi.event.transport.netty.channel.StandardChannelInitializer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Client SSL Standard Channel Initializer supporting TLS with SSLContext configuration
 * @param <T> Channel Type
 */
public class ClientSslStandardChannelInitializer<T extends Channel> extends StandardChannelInitializer<T> {
    private final SSLContext sslContext;

    /**
     * Client SSL Channel Initializer with handlers and SSLContext
     *
     * @param handlerSupplier Channel Handler Supplier
     * @param sslContext SSLContext
     */
    public ClientSslStandardChannelInitializer(final Supplier<List<ChannelHandler>> handlerSupplier, final SSLContext sslContext) {
        super(handlerSupplier);
        this.sslContext = Objects.requireNonNull(sslContext, "SSLContext is required");
    }

    @Override
    protected void initChannel(final Channel channel) {
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(newSslHandler());
        super.initChannel(channel);
    }

    private SslHandler newSslHandler() {
        final SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        return new SslHandler(sslEngine);
    }
}
