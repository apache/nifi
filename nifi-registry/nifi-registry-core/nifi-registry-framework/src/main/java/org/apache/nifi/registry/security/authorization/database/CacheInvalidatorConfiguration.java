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
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Selects the active {@link CacheInvalidator} implementation based on
 * {@code nifi.registry.cluster.coordination}.
 *
 * <ul>
 *   <li>{@code database} (default) — {@link DatabaseCacheInvalidator}:
 *       increments {@code CACHE_VERSION} rows; peers poll via
 *       {@link CacheRefreshPoller}.</li>
 *   <li>{@code zookeeper} — {@link ZkCacheInvalidator}: updates ZNodes;
 *       peers receive immediate push notifications via {@code CuratorCache}
 *       watchers.</li>
 * </ul>
 */
@Configuration
public class CacheInvalidatorConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheInvalidatorConfiguration.class);

    @Bean
    public CacheInvalidator cacheInvalidator(
            final NiFiRegistryProperties properties,
            final DataSource dataSource,
            final ObjectProvider<CuratorFramework> curatorProvider) {

        if (properties.isClusterEnabled() && properties.isZooKeeperCoordination()) {
            LOGGER.info("Cluster coordination=zookeeper; creating ZkCacheInvalidator.");
            return new ZkCacheInvalidator(curatorProvider.getObject(), properties);
        }

        LOGGER.info("Cluster coordination=database; creating DatabaseCacheInvalidator.");
        return new DatabaseCacheInvalidator(dataSource);
    }
}
