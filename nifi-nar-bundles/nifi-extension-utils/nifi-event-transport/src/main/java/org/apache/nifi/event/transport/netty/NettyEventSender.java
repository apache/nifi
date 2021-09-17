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

/**
 * Netty Event Sender with Channel Pool
 *
 * @param <T> Event Type
 */
class NettyEventSender<T> implements EventSender<T> {
    private final EventLoopGroup group;

    private final ChannelPool channelPool;

    private final SocketAddress remoteAddress;

    private boolean singleEventPerConnection;

    /**
     * Netty Channel Event Sender with Event Loop Group and Channel Pool
     *
     * @param group Event Loop Group
     * @param channelPool Channel Pool
     * @param remoteAddress Remote Address
     * @param singleEventPerConnection If true, send a single event per connection, and then close it.
     */
    NettyEventSender(final EventLoopGroup group, final ChannelPool channelPool, final SocketAddress remoteAddress, final boolean singleEventPerConnection) {
        this.group = group;
        this.channelPool = channelPool;
        this.remoteAddress = remoteAddress;
        this.singleEventPerConnection = singleEventPerConnection;
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
            group.shutdownGracefully().syncUninterruptibly();
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
