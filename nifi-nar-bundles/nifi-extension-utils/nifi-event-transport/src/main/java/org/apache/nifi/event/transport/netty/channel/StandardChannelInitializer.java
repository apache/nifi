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
package org.apache.nifi.event.transport.netty.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.WriteTimeoutHandler;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Standard Channel Initializer
 * @param <T> Channel Type
 */
public class StandardChannelInitializer<T extends Channel> extends ChannelInitializer<T> {
    private final Supplier<List<ChannelHandler>> handlerSupplier;

    private Duration writeTimeout = Duration.ofSeconds(30);

    /**
     * Standard Channel Initializer with handlers
     *
     * @param handlerSupplier Channel Handler Supplier
     */
    public StandardChannelInitializer(final Supplier<List<ChannelHandler>> handlerSupplier) {
        this.handlerSupplier = Objects.requireNonNull(handlerSupplier);
    }

    /**
     * Set Timeout for Write operations
     *
     * @param writeTimeout Write Timeout
     */
    public void setWriteTimeout(final Duration writeTimeout) {
        this.writeTimeout = Objects.requireNonNull(writeTimeout);
    }

    @Override
    protected void initChannel(Channel channel) {
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new WriteTimeoutHandler(writeTimeout.toMillis(), TimeUnit.MILLISECONDS));
        handlerSupplier.get().forEach(pipeline::addLast);
    }
}
