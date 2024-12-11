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

import java.io.File;
import javax.net.ssl.SSLContext;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.server.set.StandardSetCacheServer;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.ssl.SSLContextProvider;

@Tags({"distributed", "set", "distinct", "cache", "server"})
@CapabilityDescription("Provides a set (collection of unique values) cache that can be accessed over a socket. "
        + "Interaction with this service is typically accomplished via a DistributedSetCacheClient service.")
public class SetCacheServer extends AbstractCacheServer {

    @Override
    protected CacheServer createCacheServer(final ConfigurationContext context) {
        final int port = context.getProperty(PORT).asInteger();
        final String persistencePath = context.getProperty(PERSISTENCE_PATH).getValue();
        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final int maxSize = context.getProperty(MAX_CACHE_ENTRIES).asInteger();
        final String evictionPolicyName = context.getProperty(EVICTION_POLICY).getValue();
        final int maxReadSize = context.getProperty(MAX_READ_SIZE).asDataSize(DataUnit.B).intValue();

        final SSLContext sslContext = sslContextProvider == null ? null : sslContextProvider.createContext();

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

            return new StandardSetCacheServer(getLogger(), getIdentifier(), sslContext, port, maxSize, evictionPolicy, persistenceDir, maxReadSize);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
