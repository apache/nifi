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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.RestrictedSSLContextService;

public abstract class DistributedCacheServer extends AbstractControllerService {

    public static final String EVICTION_STRATEGY_LFU = "Least Frequently Used";
    public static final String EVICTION_STRATEGY_LRU = "Least Recently Used";
    public static final String EVICTION_STRATEGY_FIFO = "First In, First Out";

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .description("The port to listen on for incoming connections")
        .required(true)
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .defaultValue("4557")
        .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .description("If specified, this service will be used to create an SSL Context that will be used "
            + "to secure communications; if not specified, communications will not be secure")
        .required(false)
        .identifiesControllerService(RestrictedSSLContextService.class)
        .build();
    public static final PropertyDescriptor MAX_CACHE_ENTRIES = new PropertyDescriptor.Builder()
        .name("Maximum Cache Entries")
        .description("The maximum number of cache entries that the cache can hold")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("10000")
        .build();
    public static final PropertyDescriptor EVICTION_POLICY = new PropertyDescriptor.Builder()
        .name("Eviction Strategy")
        .description("Determines which strategy should be used to evict values from the cache to make room for new entries")
        .required(true)
        .allowableValues(EVICTION_STRATEGY_LFU, EVICTION_STRATEGY_LRU, EVICTION_STRATEGY_FIFO)
        .defaultValue(EVICTION_STRATEGY_LFU)
        .build();
    public static final PropertyDescriptor PERSISTENCE_PATH = new PropertyDescriptor.Builder()
        .name("Persistence Directory")
        .description("If specified, the cache will be persisted in the given directory; if not specified, the cache will be in-memory only")
        .required(false)
        .addValidator(StandardValidators.createDirectoryExistsValidator(true, true))
        .build();

    private volatile CacheServer cacheServer;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PORT);
        properties.add(MAX_CACHE_ENTRIES);
        properties.add(EVICTION_POLICY);
        properties.add(PERSISTENCE_PATH);
        properties.add(SSL_CONTEXT_SERVICE);
        return properties;
    }

    @OnEnabled
    public void startServer(final ConfigurationContext context) throws IOException {
        if (cacheServer == null) {
            cacheServer = createCacheServer(context);
            cacheServer.start();
        }
    }

    @OnDisabled
    public void shutdownServer() throws IOException {
        if (cacheServer != null) {
            cacheServer.stop();
        }
        cacheServer = null;
    }

    @Override
    protected void finalize() throws Throwable {
        shutdownServer();
    }

    /**
     * @return the port that the server is listening on, or -1 if the server has not been started
     */
    public int getPort() {
        return cacheServer == null ? -1 : cacheServer.getPort();
    }

    protected abstract CacheServer createCacheServer(ConfigurationContext context);
}
