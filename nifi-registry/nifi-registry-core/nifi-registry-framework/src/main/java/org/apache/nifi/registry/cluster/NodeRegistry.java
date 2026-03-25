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

import java.util.List;
import java.util.Optional;

/**
 * Maintains the live membership of the NiFi Registry cluster and provides
 * address look-ups used by the write-replication path.
 *
 * <p>In ZooKeeper mode ({@code nifi.registry.cluster.coordination=zookeeper})
 * the active implementation ({@link ZkNodeRegistry}) registers this node as
 * an ephemeral ZNode on startup and watches for membership changes via
 * {@code CuratorCache}.
 */
public interface NodeRegistry {

    /** Returns this node's configured identifier. */
    String getSelfNodeId();

    /** Returns this node's HTTP base URL (e.g. {@code https://node1:18443}). */
    String getSelfBaseUrl();

    /** Returns addresses of all currently live cluster members including this node. */
    List<NodeAddress> getAllNodes();

    /**
     * Returns addresses of all live cluster members <em>except</em> this node.
     * Used by the leader when fanning out writes to followers.
     */
    List<NodeAddress> getOtherNodes();

    /**
     * Returns the address of the current cluster leader, or empty if the
     * leader is unknown (e.g. during startup or a ZK session loss).
     */
    Optional<NodeAddress> getLeaderAddress();
}
