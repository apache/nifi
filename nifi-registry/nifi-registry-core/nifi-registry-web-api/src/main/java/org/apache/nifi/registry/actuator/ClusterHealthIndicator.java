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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Spring Boot Actuator health indicator that surfaces cluster state.
 *
 * <p>Exposed at {@code /actuator/health} (and {@code /actuator/health/cluster}
 * when Spring Boot's grouped health is configured). Useful for load balancers
 * that need to distinguish leaders from followers, or detect a node that has
 * lost its ZooKeeper connection.
 *
 * <p>The overall status is {@code UP} as long as the node is running.
 * A node that cannot reach ZooKeeper will report {@code leader: false} but
 * remain {@code UP} — it can still serve reads. Operators should configure
 * load balancers to route writes only to the leader by checking
 * {@code details.role}.
 *
 * <p>Example output in ZooKeeper cluster mode:
 * <pre>
 * {
 *   "status": "UP",
 *   "details": {
 *     "mode":        "zookeeper",
 *     "role":        "leader",
 *     "selfId":      "node1",
 *     "leaderId":    "node1",
 *     "memberCount": 3,
 *     "members":     ["node1", "node2", "node3"]
 *   }
 * }
 * </pre>
 */
@Component
public class ClusterHealthIndicator implements HealthIndicator {

    private final NiFiRegistryProperties properties;
    private final LeaderElectionManager leaderElectionManager;
    private final NodeRegistry nodeRegistry; // null in non-ZK modes

    @Autowired
    public ClusterHealthIndicator(final NiFiRegistryProperties properties,
            final LeaderElectionManager leaderElectionManager,
            @Autowired(required = false) final NodeRegistry nodeRegistry) {
        this.properties = properties;
        this.leaderElectionManager = leaderElectionManager;
        this.nodeRegistry = nodeRegistry;
    }

    @Override
    public Health health() {
        if (!properties.isClusterEnabled()) {
            return Health.up()
                    .withDetail("mode", "standalone")
                    .build();
        }

        final boolean isLeader = leaderElectionManager.isLeader();
        final Health.Builder builder = Health.up()
                .withDetail("mode", properties.getClusterCoordination())
                .withDetail("role", isLeader ? "leader" : "follower");

        leaderElectionManager.getLeaderNodeId()
                .ifPresent(id -> builder.withDetail("leaderId", id));

        if (nodeRegistry != null) {
            final List<NodeAddress> members = nodeRegistry.getAllNodes();
            builder.withDetail("selfId", nodeRegistry.getSelfNodeId())
                    .withDetail("memberCount", members.size())
                    .withDetail("members", members.stream()
                            .map(NodeAddress::getNodeId)
                            .toList());
        }

        return builder.build();
    }
}
