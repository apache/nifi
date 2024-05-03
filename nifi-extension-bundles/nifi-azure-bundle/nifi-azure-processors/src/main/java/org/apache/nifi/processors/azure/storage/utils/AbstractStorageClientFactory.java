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
package org.apache.nifi.processors.azure.storage.utils;

import com.azure.core.http.ProxyOptions;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.logging.ComponentLog;

abstract class AbstractStorageClientFactory<CREDENTIAL, CLIENT> {

    private static final long STORAGE_CLIENT_CACHE_SIZE = 10;

    private final ComponentLog logger;
    private final ProxyOptions proxyOptions;

    private final Cache<CREDENTIAL, CLIENT> clientCache;

    protected AbstractStorageClientFactory(final ComponentLog logger, final ProxyOptions proxyOptions) {
        this.logger = logger;
        this.proxyOptions = proxyOptions;
        this.clientCache = createCache();
    }

    private Cache<CREDENTIAL, CLIENT> createCache() {
        // Beware! By default, Caffeine does not perform cleanup and evict values
        // "automatically" or instantly after a value expires. Because of that it
        // can happen that there are more elements in the cache than the maximum size.
        // See: https://github.com/ben-manes/caffeine/wiki/Cleanup
        return Caffeine.newBuilder()
                .maximumSize(STORAGE_CLIENT_CACHE_SIZE)
                .build();
    }

    /**
     * Retrieves storage client object
     *
     * @param credentialsDetails used for caching because it can contain properties that are results of an expression
     * @return CLIENT
     */
    public CLIENT getStorageClient(final CREDENTIAL credentialsDetails) {
        return clientCache.get(credentialsDetails, __ -> {
            logger.debug(credentialsDetails.getClass().getSimpleName() + " is not found in the cache with the given credentials. Creating it.");
            return createStorageClient(credentialsDetails, proxyOptions);
        });
    }

    protected abstract CLIENT createStorageClient(CREDENTIAL credentialsDetails, ProxyOptions proxyOptions);
}
