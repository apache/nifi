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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link CacheInvalidator} backed by ZooKeeper ZNode watches.
 *
 * <p>Each cache domain maps to a persistent ZNode under
 * {@code <rootNode>/cache-version/<domain>}.
 *
 * <ul>
 *   <li>{@link #notifyChanged} calls {@code setData()} on the domain ZNode,
 *       which causes ZooKeeper to fire a {@code NodeDataChanged} event on all
 *       cluster nodes that are watching — including the current node.</li>
 *   <li>{@link #watchDomain} sets up a {@link CuratorCache} (Curator 5.x
 *       persistent-watcher replacement for {@code NodeCache}) that invokes
 *       the supplied {@code onChanged} callback whenever the ZNode is created
 *       or updated.</li>
 * </ul>
 *
 * <p>Used when {@code nifi.registry.cluster.coordination=zookeeper}.
 */
public class ZkCacheInvalidator implements CacheInvalidator, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkCacheInvalidator.class);

    private final CuratorFramework client;
    private final String rootNode;
    private final List<CuratorCache> caches = new ArrayList<>();

    public ZkCacheInvalidator(final CuratorFramework client, final NiFiRegistryProperties properties) {
        this.client = client;
        final String root = properties.getZooKeeperRootNode();
        this.rootNode = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
    }

    @Override
    public void notifyChanged(final String domain) {
        final String path = domainPath(domain);
        try {
            client.setData().forPath(path, new byte[0]);
            LOGGER.debug("Bumped ZK cache-version node for domain '{}'.", domain);
        } catch (final KeeperException.NoNodeException nne) {
            // Node doesn't exist yet — create it (first write after a fresh cluster start)
            try {
                client.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
                LOGGER.debug("Created ZK cache-version node for domain '{}'.", domain);
            } catch (final KeeperException.NodeExistsException nee) {
                // Lost a creation race with another node — retry setData
                try {
                    client.setData().forPath(path, new byte[0]);
                } catch (final Exception e) {
                    LOGGER.warn("Failed to notify ZK cache change for domain '{}': {}", domain, e.getMessage());
                }
            } catch (final Exception e) {
                LOGGER.warn("Failed to create ZK cache-version node for domain '{}': {}", domain, e.getMessage());
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to notify ZK cache change for domain '{}': {}", domain, e.getMessage());
        }
    }

    @Override
    public void watchDomain(final String domain, final Runnable onChanged) {
        final String path = domainPath(domain);

        final CuratorCache cache = CuratorCache.build(client, path);
        cache.listenable().addListener((type, oldData, newData) -> {
            if (type == CuratorCacheListener.Type.NODE_CHANGED
                    || type == CuratorCacheListener.Type.NODE_CREATED) {
                LOGGER.debug("ZK cache-version changed for domain '{}' ({}); triggering refresh.", domain, type);
                try {
                    onChanged.run();
                } catch (final Exception e) {
                    LOGGER.error("Error running cache refresh callback for domain '{}'", domain, e);
                }
            }
        });

        cache.start();
        caches.add(cache);
        LOGGER.info("Watching ZK cache-version node '{}' for domain '{}'.", path, domain);
    }

    @Override
    public void destroy() {
        for (final CuratorCache cache : caches) {
            try {
                cache.close();
            } catch (final Exception e) {
                LOGGER.debug("Exception closing CuratorCache", e);
            }
        }
        caches.clear();
    }

    private String domainPath(final String domain) {
        return rootNode + "/cache-version/" + domain;
    }
}
