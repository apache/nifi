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
package org.apache.nifi.distributed.cache.server;

import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.util.ClientAuth;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.util.Objects;

/**
 * Abstract Event Cache Server with standard lifecycle methods
 */
public abstract class EventCacheServer implements CacheServer {
    private static final InetAddress ALL_ADDRESSES = null;

    private final ComponentLog log;

    private volatile int port;

    private EventServer eventServer;

    public EventCacheServer(
            final ComponentLog log,
            final int port
    ) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.port = port;
    }

    /**
     * Start Server
     *
     */
    @Override
    public void start() {
        eventServer = createEventServer();
        port = eventServer.getListeningPort();
        log.info("Started Cache Server Port [{}]", port);
    }

    /**
     * Stop Server
     *
     */
    @Override
    public void stop() {
        if (eventServer == null) {
            log.info("Server not running");
        } else {
            eventServer.shutdown();
        }

        log.info("Stopped Cache Server Port [{}]", port);
    }

    /**
     * Get Server Port Number
     *
     * @return Port Number
     */
    @Override
    public int getPort() {
        return port;
    }

    /**
     * Create Event Server Factory with standard properties
     *
     * @param identifier Component Identifier
     * @param sslContext SSL Context is null when not configured
     * @return Netty Event Server Factory
     */
    protected NettyEventServerFactory createEventServerFactory(final String identifier, final SSLContext sslContext) {
        final NettyEventServerFactory eventServerFactory = new NettyEventServerFactory(ALL_ADDRESSES, port, TransportProtocol.TCP);
        eventServerFactory.setSslContext(sslContext);
        eventServerFactory.setClientAuth(ClientAuth.REQUIRED);

        final String threadNamePrefix = String.format("%s[%s]", getClass().getSimpleName(), identifier);
        eventServerFactory.setThreadNamePrefix(threadNamePrefix);

        eventServerFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        eventServerFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());

        return eventServerFactory;
    }

    /**
     * Create Event Server
     *
     * @return Event Server
     */
    protected abstract EventServer createEventServer();
}
