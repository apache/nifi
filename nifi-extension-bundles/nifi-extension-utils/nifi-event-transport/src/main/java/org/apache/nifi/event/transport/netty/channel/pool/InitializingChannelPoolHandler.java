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
package org.apache.nifi.event.transport.netty.channel.pool;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.AbstractChannelPoolHandler;

import java.util.Objects;

/**
 * Initializing Channel Pool Handler adds Channel Initializer when a Channel is created
 */
public class InitializingChannelPoolHandler extends AbstractChannelPoolHandler {
    private final ChannelInitializer<Channel> channelInitializer;

    /**
     * Initializing Channel Pool Handler
     *
     * @param channelInitializer Channel Initializer
     */
    public InitializingChannelPoolHandler(final ChannelInitializer<Channel> channelInitializer) {
        this.channelInitializer = Objects.requireNonNull(channelInitializer);
    }

    /**
     * Connect Channel when created
     *
     * @param channel Channel to be connected
     */
    @Override
    public void channelCreated(final Channel channel) {
        channel.pipeline().addLast(channelInitializer);
    }
}
