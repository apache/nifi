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
package org.apache.nifi.registry.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;

/**
 * Selects the active {@link LeaderElectionManager} implementation.
 *
 * <ul>
 *   <li>{@code nifi.registry.cluster.coordination=database} (default) —
 *       creates {@link DatabaseLeaderElectionManager}</li>
 *   <li>{@code nifi.registry.cluster.coordination=zookeeper} —
 *       creates {@link ZkLeaderElectionManager} using the
 *       {@link CuratorFramework} bean produced by {@link ZkClientFactory}</li>
 * </ul>
 *
 * <p>This configuration class owns the lifecycle of both implementations.
 * {@code DatabaseLeaderElectionManager} and {@code ZkLeaderElectionManager}
 * are no longer annotated with {@code @Component} — they are only instantiated
 * here, preventing accidental double-registration.
 */
@Configuration
@Import(ZkClientFactory.class)
public class LeaderElectionConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionConfiguration.class);

    @Bean
    public LeaderElectionManager leaderElectionManager(
            final NiFiRegistryProperties properties,
            final DataSource dataSource,
            // Optional — only resolved when coordination=zookeeper
            final org.springframework.beans.factory.ObjectProvider<CuratorFramework> curatorProvider) {

        if (properties.isClusterEnabled() && properties.isZooKeeperCoordination()) {
            LOGGER.info("Cluster coordination=zookeeper; creating ZkLeaderElectionManager.");
            final CuratorFramework curator = curatorProvider.getObject();
            final ZkLeaderElectionManager manager = new ZkLeaderElectionManager(curator, properties);
            manager.start();
            return manager;
        }

        LOGGER.info("Cluster coordination=database; creating DatabaseLeaderElectionManager.");
        final DatabaseLeaderElectionManager manager = new DatabaseLeaderElectionManager(dataSource, properties);
        manager.start();
        return manager;
    }
}
