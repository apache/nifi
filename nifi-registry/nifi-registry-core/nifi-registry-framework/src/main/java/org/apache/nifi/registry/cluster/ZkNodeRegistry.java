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
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ZooKeeper-backed implementation of {@link NodeRegistry}.
 *
 * <p>Each cluster member registers itself as an <em>ephemeral</em> ZNode at
 * {@code <rootNode>/members/<nodeId>} with the node's HTTP base URL as the
 * node data. Ephemeral nodes are automatically removed by ZooKeeper when the
 * session expires, giving us free liveness detection.
 *
 * <p>A {@code CuratorCache} watches the {@code /members} subtree so the local
 * membership map is kept up-to-date in real time.
 *
 * <p>This class is instantiated by {@link ReplicationConfiguration} when
 * {@code nifi.registry.cluster.coordination=zookeeper}.
 */
public class ZkNodeRegistry implements NodeRegistry, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkNodeRegistry.class);

    private final CuratorFramework client;
    private final String selfNodeId;
    private final String selfBaseUrl;
    private final String membersPath;
    private final LeaderElectionManager leaderElectionManager;

    /** Live membership: nodeId → base URL. Updated by the CuratorCache listener. */
    private final ConcurrentHashMap<String, String> members = new ConcurrentHashMap<>();

    private CuratorCache memberCache;

    public ZkNodeRegistry(final CuratorFramework client,
            final String rootNode,
            final String selfNodeId,
            final String selfBaseUrl,
            final LeaderElectionManager leaderElectionManager) {
        this.client = client;
        this.selfNodeId = selfNodeId;
        this.selfBaseUrl = selfBaseUrl;
        this.membersPath = rootNode + (rootNode.endsWith("/") ? "" : "/") + "members";
        this.leaderElectionManager = leaderElectionManager;
    }

    public void start() {
        LOGGER.info("ZkNodeRegistry starting: registering node '{}' at {} under '{}'.",
                selfNodeId, selfBaseUrl, membersPath);

        // 1. Seed the local map with the current ZK snapshot, then register self.
        loadInitialMembers();
        registerSelf();

        // 2. CuratorCache keeps the map current for subsequent changes.
        memberCache = CuratorCache.build(client, membersPath);
        memberCache.listenable().addListener(this::onMemberEvent);
        memberCache.start();
    }

    @Override
    public void destroy() {
        LOGGER.info("ZkNodeRegistry shutting down for node '{}'.", selfNodeId);
        if (memberCache != null) {
            try {
                memberCache.close();
            } catch (final Exception e) {
                LOGGER.debug("Exception closing CuratorCache", e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // NodeRegistry
    // -------------------------------------------------------------------------

    @Override
    public String getSelfNodeId() {
        return selfNodeId;
    }

    @Override
    public String getSelfBaseUrl() {
        return selfBaseUrl;
    }

    @Override
    public List<NodeAddress> getAllNodes() {
        return members.entrySet().stream()
                .map(e -> new NodeAddress(e.getKey(), e.getValue()))
                .toList();
    }

    @Override
    public List<NodeAddress> getOtherNodes() {
        return members.entrySet().stream()
                .filter(e -> !selfNodeId.equals(e.getKey()))
                .map(e -> new NodeAddress(e.getKey(), e.getValue()))
                .toList();
    }

    @Override
    public Optional<NodeAddress> getLeaderAddress() {
        return leaderElectionManager.getLeaderNodeId()
                .flatMap(leaderId -> Optional.ofNullable(members.get(leaderId))
                        .map(url -> new NodeAddress(leaderId, url)));
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void registerSelf() {
        final String path = membersPath + "/" + selfNodeId;
        final byte[] data = selfBaseUrl.getBytes(StandardCharsets.UTF_8);
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, data);
            members.put(selfNodeId, selfBaseUrl);
        } catch (final NodeExistsException e) {
            // Can happen if previous session's ephemeral node hasn't expired yet.
            try {
                client.setData().forPath(path, data);
                members.put(selfNodeId, selfBaseUrl);
            } catch (final Exception ex) {
                LOGGER.warn("Failed to update stale member node for '{}': {}", selfNodeId, ex.getMessage());
            }
        } catch (final Exception e) {
            LOGGER.error("Failed to register member node for '{}': {}", selfNodeId, e.getMessage());
        }
    }

    private void loadInitialMembers() {
        try {
            for (final String childId : client.getChildren().forPath(membersPath)) {
                try {
                    final byte[] data = client.getData().forPath(membersPath + "/" + childId);
                    if (data != null && data.length > 0) {
                        members.put(childId, new String(data, StandardCharsets.UTF_8));
                    }
                } catch (final Exception e) {
                    LOGGER.debug("Skipping member node '{}' during initial load: {}", childId, e.getMessage());
                }
            }
        } catch (final Exception e) {
            // Path may not exist yet if this is the first node.
            LOGGER.debug("Members path '{}' does not exist yet; starting with empty membership.", membersPath);
        }
    }

    private void onMemberEvent(final CuratorCacheListener.Type type,
            final ChildData oldData, final ChildData newData) {
        switch (type) {
            case NODE_CREATED, NODE_CHANGED -> {
                if (newData != null && newData.getData() != null && newData.getData().length > 0) {
                    final String nodeId = extractNodeId(newData.getPath());
                    if (!nodeId.isEmpty()) {
                        final String baseUrl = new String(newData.getData(), StandardCharsets.UTF_8);
                        members.put(nodeId, baseUrl);
                        LOGGER.info("Cluster member registered/updated: '{}' at {}.", nodeId, baseUrl);
                    }
                }
            }
            case NODE_DELETED -> {
                if (oldData != null) {
                    final String nodeId = extractNodeId(oldData.getPath());
                    if (!nodeId.isEmpty()) {
                        members.remove(nodeId);
                        LOGGER.info("Cluster member removed: '{}'.", nodeId);
                    }
                }
            }
        }
    }

    /**
     * Extracts the last path segment — the nodeId — from a ZNode path like
     * {@code /nifi-registry/members/<nodeId>}. Returns empty string for the
     * parent path itself (which CuratorCache also fires events for).
     */
    private String extractNodeId(final String path) {
        if (path == null || path.equals(membersPath)) {
            return "";
        }
        final int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : "";
    }
}
