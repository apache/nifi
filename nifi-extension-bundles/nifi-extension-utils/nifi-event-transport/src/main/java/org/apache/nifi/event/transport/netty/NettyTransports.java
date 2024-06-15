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
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;


public class NettyTransports {
  public record NettyTransport(String name,
                               boolean isAvailable,
                               Class<? extends EventLoopGroup> eventLoopGroupClass,
                               Class<? extends ServerSocketChannel> serverSocketClass,
                               Class<? extends SocketChannel> socketChannelClass,
                               Class<? extends DatagramChannel> datagramChannelClass) {
    public EventLoopGroup createEventLoopGroup(int workerThreads, ThreadFactory threadFactory) {
      try {
        Constructor<? extends EventLoopGroup> c = eventLoopGroupClass.getConstructor(Integer.TYPE, ThreadFactory.class);
        return c.newInstance(workerThreads, threadFactory);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static final NettyTransport EPOLL = new NettyTransport("epoll", Epoll.isAvailable(),
      EpollEventLoopGroup.class,
      EpollServerSocketChannel.class,
      EpollSocketChannel.class,
      EpollDatagramChannel.class);

  public static final NettyTransport KQUEUE = new NettyTransport("kqueue", KQueue.isAvailable(),
      KQueueEventLoopGroup.class,
      KQueueServerSocketChannel.class,
      KQueueSocketChannel.class,
      KQueueDatagramChannel.class);

  public static final NettyTransport NIO = new NettyTransport("nio", true,
      NioEventLoopGroup.class,
      NioServerSocketChannel.class,
      NioSocketChannel.class,
      NioDatagramChannel.class);

  public static final List<NettyTransport> AVAILABLE_TRANSPORTS = List.of(EPOLL, KQUEUE, NIO)
      .stream()
      .filter(transport -> transport.isAvailable())
      .collect(Collectors.toUnmodifiableList());

  public static final List<String> AVAILABLE_TRANSPORT_NAMES = AVAILABLE_TRANSPORTS
      .stream()
      .map(transport -> transport.name())
      .collect(Collectors.toUnmodifiableList());

  public static NettyTransport getDefaultNettyTransport() {
    return AVAILABLE_TRANSPORTS.getFirst();
  }

}
