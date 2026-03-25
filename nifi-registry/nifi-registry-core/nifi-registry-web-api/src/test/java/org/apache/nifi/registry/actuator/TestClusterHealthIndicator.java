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
package org.apache.nifi.registry.actuator;

import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.cluster.NodeAddress;
import org.apache.nifi.registry.cluster.NodeRegistry;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Pure Mockito unit tests for {@link ClusterHealthIndicator}.
 */
@ExtendWith(MockitoExtension.class)
public class TestClusterHealthIndicator {

    @Mock
    private NiFiRegistryProperties properties;

    @Mock
    private LeaderElectionManager leaderElectionManager;

    @Mock
    private NodeRegistry nodeRegistry;

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * In standalone mode (cluster disabled) health should be UP with mode=standalone.
     */
    @Test
    public void testStandaloneModeReturnsUp() {
        when(properties.isClusterEnabled()).thenReturn(false);

        final ClusterHealthIndicator indicator = new ClusterHealthIndicator(
                properties, leaderElectionManager, null);

        final Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("standalone", health.getDetails().get("mode"));
    }

    /**
     * Cluster mode, follower, nodeRegistry null: UP with role=follower.
     */
    @Test
    public void testClusterModeFollowerNoRegistry() {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.getClusterCoordination()).thenReturn("database");
        when(leaderElectionManager.isLeader()).thenReturn(false);
        when(leaderElectionManager.getLeaderNodeId()).thenReturn(Optional.empty());

        final ClusterHealthIndicator indicator = new ClusterHealthIndicator(
                properties, leaderElectionManager, null);

        final Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("follower", health.getDetails().get("role"));
    }

    /**
     * Cluster mode, leader, nodeRegistry null: UP with role=leader.
     */
    @Test
    public void testClusterModeLeaderNoRegistry() {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.getClusterCoordination()).thenReturn("database");
        when(leaderElectionManager.isLeader()).thenReturn(true);
        when(leaderElectionManager.getLeaderNodeId()).thenReturn(Optional.empty());

        final ClusterHealthIndicator indicator = new ClusterHealthIndicator(
                properties, leaderElectionManager, null);

        final Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("leader", health.getDetails().get("role"));
    }

    /**
     * Cluster mode with a NodeRegistry present: memberCount and members list
     * should appear in health details.
     */
    @Test
    public void testClusterModeWithRegistry() {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.getClusterCoordination()).thenReturn("zookeeper");
        when(leaderElectionManager.isLeader()).thenReturn(true);
        when(leaderElectionManager.getLeaderNodeId()).thenReturn(Optional.empty());

        final List<NodeAddress> members = List.of(
                new NodeAddress("node1", "http://node1:18080"),
                new NodeAddress("node2", "http://node2:18080"),
                new NodeAddress("node3", "http://node3:18080"));
        when(nodeRegistry.getAllNodes()).thenReturn(members);
        when(nodeRegistry.getSelfNodeId()).thenReturn("node1");

        final ClusterHealthIndicator indicator = new ClusterHealthIndicator(
                properties, leaderElectionManager, nodeRegistry);

        final Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals(3, health.getDetails().get("memberCount"));

        @SuppressWarnings("unchecked")
        final List<String> memberIds = (List<String>) health.getDetails().get("members");
        assertNotNull(memberIds);
        assertEquals(3, memberIds.size());
        assertTrue(memberIds.contains("node1"));
        assertTrue(memberIds.contains("node2"));
        assertTrue(memberIds.contains("node3"));
    }

    /**
     * When getLeaderNodeId() returns a non-empty optional, the leaderId detail
     * should be present in the health response.
     */
    @Test
    public void testLeaderIdPresentWhenAvailable() {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.getClusterCoordination()).thenReturn("zookeeper");
        when(leaderElectionManager.isLeader()).thenReturn(false);
        when(leaderElectionManager.getLeaderNodeId()).thenReturn(Optional.of("node1"));

        final ClusterHealthIndicator indicator = new ClusterHealthIndicator(
                properties, leaderElectionManager, null);

        final Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("node1", health.getDetails().get("leaderId"));
    }
}
