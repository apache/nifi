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
package org.apache.nifi.registry.security.authorization.database;

/**
 * Abstraction over the mechanism used to signal cache invalidation across
 * cluster nodes when authorization data changes.
 *
 * <p>Two implementations are provided:
 * <ul>
 *   <li>{@link DatabaseCacheInvalidator} — increments a row in the
 *       {@code CACHE_VERSION} table; peers discover the change via
 *       {@link CacheRefreshPoller} (used when
 *       {@code nifi.registry.cluster.coordination=database}).</li>
 *   <li>{@link ZkCacheInvalidator} — updates a ZooKeeper ZNode and uses
 *       a persistent {@code CuratorCache} watcher to push invalidation to
 *       all peers immediately (used when
 *       {@code nifi.registry.cluster.coordination=zookeeper}).</li>
 * </ul>
 */
public interface CacheInvalidator {

    /**
     * Signals to all other cluster nodes that the cache for {@code domain}
     * is stale and must be reloaded.  Called after every write to the
     * corresponding authorization store.
     *
     * @param domain one of {@link CacheRefreshPoller#DOMAIN_ACCESS_POLICIES}
     *               or {@link CacheRefreshPoller#DOMAIN_USER_GROUPS}
     */
    void notifyChanged(String domain);

    /**
     * Registers a callback that is invoked whenever another cluster node
     * signals a cache change for {@code domain}.
     *
     * <p>For the database implementation this is a no-op because
     * {@link CacheRefreshPoller} drives refreshes directly.  For the
     * ZooKeeper implementation a {@code CuratorCache} watcher is set up
     * here so that refreshes are push-based.
     *
     * @param domain     the cache domain to watch
     * @param onChanged  callback invoked on the watcher thread when a change is detected
     */
    void watchDomain(String domain, Runnable onChanged);
}
