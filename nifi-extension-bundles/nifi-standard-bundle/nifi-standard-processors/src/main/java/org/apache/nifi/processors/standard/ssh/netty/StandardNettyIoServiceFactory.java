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
import io.netty.util.concurrent.Future;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.sshd.common.FactoryManager;
import org.apache.sshd.common.future.CloseFuture;
import org.apache.sshd.common.io.IoConnector;
import org.apache.sshd.common.io.IoHandler;
import org.apache.sshd.netty.NettyIoServiceFactory;

import java.time.Duration;
import java.util.Objects;

/**
 * Standard implementation supporting Proxy Configuration for IO Connector
 */
class StandardNettyIoServiceFactory extends NettyIoServiceFactory {
    private final ProxyConfiguration proxyConfiguration;

    private final Duration socketTimeout;

    StandardNettyIoServiceFactory(
            final FactoryManager manager,
            final EventLoopGroup eventLoopGroup,
            final ProxyConfiguration proxyConfiguration,
            final Duration socketTimeout
    ) {
        super(manager, eventLoopGroup);
        this.proxyConfiguration = Objects.requireNonNull(proxyConfiguration, "Proxy Configuration required");
        this.socketTimeout = Objects.requireNonNull(socketTimeout, "Socket Timeout required");
    }

    @Override
    public IoConnector createConnector(final IoHandler ioHandler) {
        return new StandardNettyIoConnector(this, ioHandler, eventLoopGroup, proxyConfiguration, socketTimeout, manager);
    }

    /**
     * Close configured Netty Event Loop Group
     *
     * @return Close Future with configured listeners
     */
    @Override
    protected CloseFuture doCloseGracefully() {
        final Future<?> shutdownFuture = eventLoopGroup.shutdownGracefully();
        shutdownFuture.addListener(future -> closeFuture.setClosed());
        return closeFuture;
    }
}
