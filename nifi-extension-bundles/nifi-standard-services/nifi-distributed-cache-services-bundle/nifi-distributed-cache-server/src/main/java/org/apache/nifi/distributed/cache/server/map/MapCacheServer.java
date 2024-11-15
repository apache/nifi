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
package org.apache.nifi.distributed.cache.server.map;

import java.io.File;
import java.io.IOException;
import javax.net.ssl.SSLContext;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.server.CacheServer;
import org.apache.nifi.distributed.cache.server.AbstractCacheServer;
import org.apache.nifi.distributed.cache.server.EvictionPolicy;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.ssl.SSLContextProvider;

@Tags({"distributed", "cluster", "map", "cache", "server", "key/value"})
@CapabilityDescription("Provides a map (key/value) cache that can be accessed over a socket. Interaction with this service"
        + " is typically accomplished via a Map Cache Client Service.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.MapCacheClientService"})
public class MapCacheServer extends AbstractCacheServer {

    @Override
    protected CacheServer createCacheServer(final ConfigurationContext context) {
        final int port = context.getProperty(PORT).asInteger();
        final String persistencePath = context.getProperty(PERSISTENCE_PATH).getValue();
        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final int maxSize = context.getProperty(MAX_CACHE_ENTRIES).asInteger();
        final String evictionPolicyName = context.getProperty(EVICTION_POLICY).getValue();
        final int maxReadSize = context.getProperty(MAX_READ_SIZE).asDataSize(DataUnit.B).intValue();

        final SSLContext sslContext;
        if (sslContextProvider == null) {
            sslContext = null;
        } else {
            sslContext = sslContextProvider.createContext();
        }

        final EvictionPolicy evictionPolicy;
        switch (evictionPolicyName) {
            case EVICTION_STRATEGY_FIFO:
                evictionPolicy = EvictionPolicy.FIFO;
                break;
            case EVICTION_STRATEGY_LFU:
                evictionPolicy = EvictionPolicy.LFU;
                break;
            case EVICTION_STRATEGY_LRU:
                evictionPolicy = EvictionPolicy.LRU;
                break;
            default:
                throw new IllegalArgumentException("Illegal Eviction Policy: " + evictionPolicyName);
        }

        try {
            final File persistenceDir = persistencePath == null ? null : new File(persistencePath);

            return createMapCacheServer(port, maxSize, sslContext, evictionPolicy, persistenceDir, maxReadSize);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected CacheServer createMapCacheServer(
            final int port,
            final int maxSize,
            final SSLContext sslContext,
            final EvictionPolicy evictionPolicy,
            final File persistenceDir,
            final int maxReadSize
    ) throws IOException {
        return new StandardMapCacheServer(
                getLogger(),
                getIdentifier(),
                sslContext,
                port,
                maxSize,
                evictionPolicy,
                persistenceDir,
                maxReadSize
        );
    }
}
