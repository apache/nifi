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

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.net.InetAddress;

/**
 * Creates the {@link NodeRegistry} and {@link ReplicationClient} beans used
 * by the write-replication filter in the web layer.
 *
 * <p>Both beans are only active in ZooKeeper cluster mode
 * ({@code nifi.registry.cluster.coordination=zookeeper}). In all other modes
 * ({@code database} or standalone) {@code null} is returned so the filter
 * passes every request through unchanged.
 *
 * <p>{@code @ConditionalOnProperty} is intentionally <em>not</em> used here
 * because {@link NiFiRegistryProperties} is not exposed to the Spring
 * {@code Environment} (it is loaded before the Spring context via the
 * bootstrap mechanism). Factory-bean pattern with direct property checks is
 * used throughout the codebase instead.
 */
@Configuration
@Import(ZkClientFactory.class)
public class ReplicationConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationConfiguration.class);

    @Autowired
    private NiFiRegistryProperties properties;

    @Autowired
    private LeaderElectionManager leaderElectionManager;

    /**
     * Provides the ZK-backed node registry when in ZooKeeper cluster mode,
     * or {@code null} (no-op) otherwise.
     */
    @Bean
    public NodeRegistry nodeRegistry(final ObjectProvider<CuratorFramework> curatorProvider) {
        if (!properties.isClusterEnabled() || !properties.isZooKeeperCoordination()) {
            LOGGER.debug("NodeRegistry not created: ZooKeeper coordination is not enabled.");
            return null;
        }

        final String selfNodeId = resolveSelfNodeId();
        final String selfBaseUrl = properties.getClusterNodeAddress();
        if (StringUtils.isBlank(selfBaseUrl)) {
            LOGGER.warn("NodeRegistry not created: '{}' is not configured. "
                    + "Set nifi.registry.cluster.node.address in nifi-registry.properties.",
                    NiFiRegistryProperties.CLUSTER_NODE_ADDRESS);
            return null;
        }

        final CuratorFramework curator = curatorProvider.getObject();
        final ZkNodeRegistry registry = new ZkNodeRegistry(
                curator,
                properties.getZooKeeperRootNode(),
                selfNodeId,
                selfBaseUrl,
                leaderElectionManager);
        registry.start();
        return registry;
    }

    /**
     * Provides the HTTP replication client when in ZooKeeper cluster mode,
     * or {@code null} otherwise.
     */
    @Bean
    public ReplicationClient replicationClient() {
        if (!properties.isClusterEnabled() || !properties.isZooKeeperCoordination()) {
            LOGGER.debug("ReplicationClient not created: ZooKeeper coordination is not enabled.");
            return null;
        }

        final String authToken = properties.getClusterNodeInternalAuthToken();
        if (StringUtils.isBlank(authToken)) {
            LOGGER.warn("ReplicationClient not created: '{}' is not configured. "
                    + "Set nifi.registry.cluster.node.internal.auth.token in nifi-registry.properties.",
                    NiFiRegistryProperties.CLUSTER_NODE_INTERNAL_AUTH_TOKEN);
            return null;
        }

        return new HttpReplicationClient(authToken);
    }

    private String resolveSelfNodeId() {
        final String configured = properties.getClusterNodeIdentifier();
        if (!StringUtils.isBlank(configured)) {
            return configured;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final Exception e) {
            LOGGER.warn("Unable to resolve hostname; using 'unknown' as node identifier.", e);
            return "unknown";
        }
    }
}
