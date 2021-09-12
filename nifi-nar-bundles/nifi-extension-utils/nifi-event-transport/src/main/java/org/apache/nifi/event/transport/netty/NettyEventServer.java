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
package org.apache.nifi.event.transport.netty;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.apache.nifi.event.transport.EventServer;

import java.util.concurrent.TimeUnit;

/**
 * Netty Event Server
 */
class NettyEventServer implements EventServer {
    static final long SHUTDOWN_QUIET_PERIOD = 2000;  // netty default
    static final long SHUTDOWN_TIMEOUT = 15000;  // netty default
    static final TimeUnit SHUTDOWN_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final EventLoopGroup group;

    private final Channel channel;

    private final long quietPeriod;

    private final long timeout;

    private final TimeUnit timeUnit;

    /**
     * Netty Event Server with Event Loop Group and bound Channel
     *
     * @param group Event Loop Group
     * @param channel Bound Channel
     */
    NettyEventServer(final EventLoopGroup group, final Channel channel) {
        this(group, channel, SHUTDOWN_QUIET_PERIOD, SHUTDOWN_TIMEOUT, SHUTDOWN_TIME_UNIT);
    }

    /**
     * Netty Event Server with Event Loop Group, bound Channel, and Shutdown Configuration
     *
     * @param group Event Loop Group
     * @param channel Bound Channel
     * @param quietPeriod server shutdown quiet period
     * @param timeout server shutdown timeout
     * @param timeUnit server shutdown parameter time unit
     */
    NettyEventServer(final EventLoopGroup group, final Channel channel,
                     final long quietPeriod, final long timeout, final TimeUnit timeUnit) {
        this.group = group;
        this.channel = channel;
        this.quietPeriod = quietPeriod;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    /**
     * Close Channel and shutdown Event Loop Group
     */
    @Override
    public void shutdown() {
        try {
            if (channel.isOpen()) {
                channel.close().syncUninterruptibly();
            }
        } finally {
            group.shutdownGracefully(quietPeriod, timeout, timeUnit).syncUninterruptibly();
        }
    }
}
