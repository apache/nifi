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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;


public enum NettyTransport {
    NIO("nio", true, NioEventLoopGroup.class, NioSocketChannel.class, NioServerSocketChannel.class, NioDatagramChannel.class),
    EPOLL("epoll", Epoll.isAvailable(), EpollEventLoopGroup.class, EpollSocketChannel.class, EpollServerSocketChannel.class, EpollDatagramChannel.class),
    KQUEUE("kqueue", KQueue.isAvailable(), KQueueEventLoopGroup.class, KQueueSocketChannel.class, KQueueServerSocketChannel.class, KQueueDatagramChannel.class);

    private static final NettyTransport[] ALL_VALUES = values();

    private final String nettyName;
    private final boolean isAvailable;
    private final Class<? extends EventLoopGroup> eventLoopGroupClass;
    private final Class<? extends ServerSocketChannel> serverSocketChannelClass;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final Class<? extends DatagramChannel> datagramChannelClass;

    NettyTransport(
        String nettyName,
        boolean isAvailable,
        Class<? extends EventLoopGroup> eventLoopGroupClass,
        Class<? extends SocketChannel> socketChannelClass,
        Class<? extends ServerSocketChannel> serverSocketClass,
        Class<? extends DatagramChannel> datagramChannelClass) {
        this.isAvailable = isAvailable;
        this.nettyName = nettyName;
        this.eventLoopGroupClass = eventLoopGroupClass;
        this.socketChannelClass = socketChannelClass;
        this.serverSocketChannelClass = serverSocketClass;
        this.datagramChannelClass = datagramChannelClass;
    }

    public String getNettyName() {
        return this.nettyName;
    }

    public boolean isAvailable() {
        return this.isAvailable;
    }

    public Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        return this.serverSocketChannelClass;
    }

    public Class<? extends SocketChannel> getSocketChannelClass() {
        return this.socketChannelClass;
    }

    public Class<? extends DatagramChannel> getDatagramChannelClass() {
        return this.datagramChannelClass;
    }

    public Class<? extends EventLoopGroup> getEventLoopGroupClass() {
        return this.eventLoopGroupClass;
    }

    public EventLoopGroup createEventLoopGroup(int workerThreads, ThreadFactory threadFactory) {
        try {
            Constructor<? extends EventLoopGroup> c =
                eventLoopGroupClass.getConstructor(Integer.TYPE, ThreadFactory.class);
            return c.newInstance(workerThreads, threadFactory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<NettyTransport> availableTransports() {
        return Arrays.stream(ALL_VALUES)
                .filter(NettyTransport::isAvailable);
    }

    public static NettyTransport getDefault() {
        if (EPOLL.isAvailable()) {
            return EPOLL;
        }
        else if (KQUEUE.isAvailable()) {
            return KQUEUE;
        }
        return NIO;
    }
}