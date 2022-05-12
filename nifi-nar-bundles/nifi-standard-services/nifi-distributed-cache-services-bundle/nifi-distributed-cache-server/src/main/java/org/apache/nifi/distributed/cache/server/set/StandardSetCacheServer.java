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
package org.apache.nifi.distributed.cache.server.set;

import org.apache.nifi.distributed.cache.operations.SetOperation;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.distributed.cache.server.EventCacheServer;
import org.apache.nifi.distributed.cache.server.EvictionPolicy;
import org.apache.nifi.distributed.cache.server.codec.CacheOperationResultEncoder;
import org.apache.nifi.distributed.cache.server.codec.CacheRequestDecoder;
import org.apache.nifi.distributed.cache.server.codec.CacheVersionRequestHandler;
import org.apache.nifi.distributed.cache.server.codec.CacheVersionResponseEncoder;
import org.apache.nifi.distributed.cache.server.codec.SetCacheRequestHandler;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.EventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * Standard Set Cache Server implementation based on Netty
 */
public class StandardSetCacheServer extends EventCacheServer {
    private final EventServerFactory eventServerFactory;

    private final SetCache cache;

    public StandardSetCacheServer(
            final ComponentLog log,
            final String identifier,
            final SSLContext sslContext,
            final int port,
            final int maxCacheEntries,
            final EvictionPolicy evictionPolicy,
            final File persistencePath,
            final int maxReadLength
    ) throws IOException {
        super(log, port);

        final SetCache simpleCache = new SimpleSetCache(identifier, maxCacheEntries, evictionPolicy);

        if (persistencePath == null) {
            this.cache = simpleCache;
        } else {
            final PersistentSetCache persistentCache = new PersistentSetCache(identifier, persistencePath, simpleCache);
            persistentCache.restore();
            this.cache = persistentCache;
        }

        final NettyEventServerFactory nettyEventServerFactory = createEventServerFactory(identifier, sslContext);

        // Create Sharable Handlers to avoid unnecessary instantiation for each connection
        final SetCacheRequestHandler setCacheRequestHandler = new SetCacheRequestHandler(log, cache);
        final CacheVersionResponseEncoder cacheVersionResponseEncoder = new CacheVersionResponseEncoder();
        final CacheOperationResultEncoder cacheOperationResultEncoder = new CacheOperationResultEncoder();

        final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(ProtocolVersion.V1.value());
        nettyEventServerFactory.setHandlerSupplier(() ->
                Arrays.asList(
                        cacheVersionResponseEncoder,
                        cacheOperationResultEncoder,
                        new CacheRequestDecoder(log, maxReadLength, SetOperation.values()),
                        setCacheRequestHandler,
                        new CacheVersionRequestHandler(log, versionNegotiator)
                )
        );

        this.eventServerFactory = nettyEventServerFactory;
    }

    @Override
    public void stop() {
        try {
            cache.shutdown();
        } catch (final IOException e) {
            throw new UncheckedIOException("Cache Shutdown Failed", e);
        } finally {
            super.stop();
        }
    }

    @Override
    protected EventServer createEventServer() {
        return eventServerFactory.getEventServer();
    }
}
