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
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventSender;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Netty Event Sender with Channel Pool
 *
 * @param <T> Event Type
 */
class NettyEventSender<T> implements EventSender<T> {
    static final long SHUTDOWN_QUIET_PERIOD = 2000;  // netty default
    static final long SHUTDOWN_TIMEOUT = 15000;  // netty default
    static final TimeUnit SHUTDOWN_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final EventLoopGroup group;

    private final ChannelPool channelPool;

    private final SocketAddress remoteAddress;

    private final boolean singleEventPerConnection;

    private final long shutdownQuietPeriod;

    private final long shutdownTimeout;

    private final TimeUnit shutdownTimeUnit;

    /**
     * Netty Channel Event Sender with Event Loop Group and Channel Pool
     *
     * @param group Event Loop Group
     * @param channelPool Channel Pool
     * @param remoteAddress Remote Address
     * @param singleEventPerConnection If true, send a single event per connection, and then close it.
     */
    NettyEventSender(final EventLoopGroup group, final ChannelPool channelPool, final SocketAddress remoteAddress, final boolean singleEventPerConnection) {
        this(group, channelPool, remoteAddress, singleEventPerConnection, SHUTDOWN_QUIET_PERIOD, SHUTDOWN_TIMEOUT, SHUTDOWN_TIME_UNIT);
    }

    /**
     * Netty Channel Event Sender with Event Loop Group, Channel Pool, and Shutdown Configuration
     *
     * @param group Event Loop Group
     * @param channelPool Channel Pool
     * @param remoteAddress Remote Address
     * @param singleEventPerConnection If true, send a single event per connection, and then close it.
     * @param shutdownQuietPeriod server shutdown quiet period
     * @param shutdownTimeout server shutdown timeout
     * @param shutdownTimeUnit server shutdown parameter time unit
     */
    NettyEventSender(final EventLoopGroup group, final ChannelPool channelPool, final SocketAddress remoteAddress, final boolean singleEventPerConnection,
                     final long shutdownQuietPeriod, final long shutdownTimeout, final TimeUnit shutdownTimeUnit) {
        this.group = group;
        this.channelPool = channelPool;
        this.remoteAddress = remoteAddress;
        this.singleEventPerConnection = singleEventPerConnection;
        this.shutdownQuietPeriod = shutdownQuietPeriod;
        this.shutdownTimeout = shutdownTimeout;
        this.shutdownTimeUnit = shutdownTimeUnit;
    }

    /**
     * Send Event using Channel acquired from Channel Pool
     *
     * @param event Event
     */
    @Override
    public void sendEvent(final T event) {
        try {
            final Future<Channel> futureChannel = channelPool.acquire().sync();
            final Channel channel = futureChannel.get();
            try {
                final ChannelFuture channelFuture = channel.writeAndFlush(event);
                channelFuture.syncUninterruptibly();
            } finally {
                releaseChannel(channel);
            }
        } catch (final Exception e) {
            throw new EventException(getChannelMessage("Send Failed"), e);
        }
    }

    /**
     * Close Channel Pool and Event Loop Group
     */
    @Override
    public void close() {
        try {
            channelPool.close();
        } finally {
            group.shutdownGracefully(shutdownQuietPeriod, shutdownTimeout, shutdownTimeUnit).syncUninterruptibly();
        }
    }

    /**
     * String representation includes Channel Remote Address
     *
     * @return String with Channel Remote Address
     */
    @Override
    public String toString() {
        return getChannelMessage("Event Sender");
    }

    private String getChannelMessage(final String message) {
        return String.format("%s Remote Address [%s]", message, remoteAddress);
    }

    private void releaseChannel(final Channel channel) {
        if (singleEventPerConnection) {
            channel.close();
        }
        channelPool.release(channel);
    }
}
