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

/**
 * Provides leader-election semantics for NiFi Registry cluster nodes.
 *
 * <p>In a single-node deployment the implementation always returns {@code false}
 * from {@link #isLeader()} (the node is not participating in election). When
 * {@code nifi.registry.cluster.enabled=true} the active implementation performs
 * distributed leader election so that exactly one node is leader at any point
 * in time.
 *
 * <p>Two implementations are available, selected by
 * {@code nifi.registry.cluster.coordination}:
 * <ul>
 *   <li>{@code database} (default) — TTL-based lease in the {@code CLUSTER_LEADER} table</li>
 *   <li>{@code zookeeper} — Apache Curator {@code LeaderSelector} backed by ZooKeeper</li>
 * </ul>
 */
public interface LeaderElectionManager {

    /**
     * Returns {@code true} if this node currently holds the leader role.
     *
     * <p>For the database implementation the value lags by up to the heartbeat
     * interval (10 s). For the ZooKeeper implementation it is verified against
     * ZooKeeper at most every 5 seconds.
     */
    boolean isLeader();

    /**
     * Registers a listener that will be notified whenever this node gains or
     * loses the leader role. Listeners are notified on the election thread;
     * implementations must not block indefinitely.
     *
     * <p>Default implementation is a no-op so existing implementations are
     * not required to override it immediately.
     */
    default void addLeaderChangeListener(final LeaderChangeListener listener) {
    }

    /**
     * Returns the node identifier of the current cluster leader, or empty if
     * the leader is unknown (e.g. during election or ZK session loss).
     *
     * <p>Default returns empty; ZooKeeper implementation resolves via
     * {@code LeaderSelector.getLeader()}.
     */
    default java.util.Optional<String> getLeaderNodeId() {
        return java.util.Optional.empty();
    }
}
