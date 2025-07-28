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
package org.apache.nifi.processors.standard.ssh.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.sshd.common.FactoryManager;
import org.apache.sshd.common.io.AbstractIoServiceFactoryFactory;
import org.apache.sshd.common.io.IoServiceFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;

/**
 * Standard implementation supporting Proxy Configuration and Virtual Threads for Socket communication
 */
public class StandardNettyIoServiceFactoryFactory extends AbstractIoServiceFactoryFactory {

    private static final String THREAD_NAME_FORMAT = "SFTP-%s:%d";

    private static final int EVENT_LOOP_GROUP_THREADS = 1;

    private final ProxyConfiguration proxyConfiguration;

    private final EventLoopGroup eventLoopGroup;

    private final Duration socketTimeout;

    public StandardNettyIoServiceFactoryFactory(
            final InetSocketAddress remoteAddress,
            final ProxyConfiguration proxyConfiguration,
            final Duration socketTimeout
    ) {
        super(null);
        Objects.requireNonNull(remoteAddress);
        this.proxyConfiguration = Objects.requireNonNull(proxyConfiguration, "Proxy Configuration required");
        this.eventLoopGroup = createEventLoopGroup(remoteAddress);
        this.socketTimeout = Objects.requireNonNull(socketTimeout, "Socket Timeout required");
    }

    @Override
    public IoServiceFactory create(final FactoryManager manager) {
        Objects.requireNonNull(manager, "Factory Manager required");
        final IoServiceFactory ioServiceFactory = new StandardNettyIoServiceFactory(manager, eventLoopGroup, proxyConfiguration, socketTimeout);
        ioServiceFactory.setIoServiceEventListener(manager.getIoServiceEventListener());
        return ioServiceFactory;
    }

    private EventLoopGroup createEventLoopGroup(final InetSocketAddress remoteAddress) {
        final String threadName = THREAD_NAME_FORMAT.formatted(remoteAddress.getHostString(), remoteAddress.getPort());
        final ThreadFactory threadFactory = Thread.ofPlatform().name(threadName).factory();
        return new MultiThreadIoEventLoopGroup(EVENT_LOOP_GROUP_THREADS, threadFactory, NioIoHandler.newFactory());
    }
}
