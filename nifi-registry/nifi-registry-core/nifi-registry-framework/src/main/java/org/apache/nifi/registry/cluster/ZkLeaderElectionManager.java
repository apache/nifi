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
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.net.InetAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.leader.Participant;

/**
 * ZooKeeper-backed implementation of {@link LeaderElectionManager}.
 *
 * <p>Uses Apache Curator's {@link LeaderSelector} with {@code autoRequeue()} so
 * this node continuously participates in the election. The exact same pattern
 * used by NiFi's {@code CuratorLeaderElectionManager} is followed here:
 *
 * <ul>
 *   <li>Leadership is held as long as {@link ElectionListener#takeLeadership} is
 *       blocking. Returning from that method yields leadership back to Curator.</li>
 *   <li>On {@code SUSPENDED} or {@code LOST} connection state the leader flag is
 *       immediately cleared — the node never claims leadership without a live
 *       ZooKeeper session.</li>
 *   <li>{@link #isLeader()} verifies against ZooKeeper at most every
 *       {@value #VERIFY_CACHE_MS} ms to catch cases where Curator fails to
 *       interrupt the leader thread.</li>
 * </ul>
 *
 * <p>ZNode path: {@code <rootNode>/leaders/registry-leader}
 *
 * <p>This class is instantiated by {@link LeaderElectionConfiguration} when
 * {@code nifi.registry.cluster.coordination=zookeeper}.
 */
public class ZkLeaderElectionManager implements LeaderElectionManager, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkLeaderElectionManager.class);

    static final String ELECTION_PATH_SUFFIX = "leaders/registry-leader";

    /** How long {@link #isLeader()} caches the ZooKeeper-verified result (ms). */
    static final long VERIFY_CACHE_MS = TimeUnit.SECONDS.toMillis(5);

    private final String nodeId;
    private final String electionPath;
    private final List<LeaderChangeListener> listeners = new CopyOnWriteArrayList<>();

    private final ElectionListener electionListener;
    private final LeaderSelector leaderSelector;

    public ZkLeaderElectionManager(final CuratorFramework curatorClient,
            final NiFiRegistryProperties properties) {
        this.nodeId = resolveNodeId(properties);

        final String root = properties.getZooKeeperRootNode();
        this.electionPath = root + (root.endsWith("/") ? "" : "/") + ELECTION_PATH_SUFFIX;

        this.electionListener = new ElectionListener();
        this.leaderSelector = new LeaderSelector(curatorClient, electionPath, electionListener);
        this.leaderSelector.autoRequeue();
        this.leaderSelector.setId(nodeId);
    }

    public void start() {
        LOGGER.info("Starting ZkLeaderElectionManager for node '{}' at path '{}'.", nodeId, electionPath);
        leaderSelector.start();
    }

    @Override
    public void destroy() {
        LOGGER.info("Shutting down ZkLeaderElectionManager.");
        electionListener.disable();
        try {
            leaderSelector.close();
        } catch (final Exception e) {
            LOGGER.debug("Exception closing LeaderSelector (expected if not started)", e);
        }
    }

    @Override
    public boolean isLeader() {
        return electionListener.isLeader();
    }

    @Override
    public void addLeaderChangeListener(final LeaderChangeListener listener) {
        listeners.add(listener);
    }

    @Override
    public Optional<String> getLeaderNodeId() {
        try {
            final Participant leader = leaderSelector.getLeader();
            if (leader == null || StringUtils.isEmpty(leader.getId())) {
                return Optional.empty();
            }
            return Optional.of(leader.getId());
        } catch (final Exception e) {
            LOGGER.debug("Unable to resolve leader node ID from ZooKeeper: {}", e.getMessage());
            return Optional.empty();
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void notifyStartLeading() {
        for (final LeaderChangeListener l : listeners) {
            try {
                l.onStartLeading();
            } catch (final Exception e) {
                LOGGER.error("LeaderChangeListener threw on onStartLeading()", e);
            }
        }
    }

    private void notifyStopLeading() {
        for (final LeaderChangeListener l : listeners) {
            try {
                l.onStopLeading();
            } catch (final Exception e) {
                LOGGER.error("LeaderChangeListener threw on onStopLeading()", e);
            }
        }
    }

    /**
     * Asks ZooKeeper directly who the current leader is and returns whether it
     * matches this node's id.
     */
    private boolean verifyLeaderWithZk() {
        try {
            final org.apache.curator.framework.recipes.leader.Participant leader =
                    leaderSelector.getLeader();
            if (leader == null || StringUtils.isEmpty(leader.getId())) {
                return false;
            }
            return nodeId.equals(leader.getId());
        } catch (final Exception e) {
            LOGGER.warn("Failed to verify leadership with ZooKeeper for node '{}': {}", nodeId, e.getMessage());
            return false;
        }
    }

    private static String resolveNodeId(final NiFiRegistryProperties props) {
        final String configured = props.getClusterNodeIdentifier();
        if (!StringUtils.isBlank(configured)) {
            return configured;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final Exception e) {
            LOGGER.warn("Unable to resolve hostname for node identifier; using 'unknown'", e);
            return "unknown";
        }
    }

    // -------------------------------------------------------------------------
    // Inner class: ElectionListener — mirrors NiFi's CuratorLeaderElectionManager.ElectionListener
    // -------------------------------------------------------------------------

    private class ElectionListener extends LeaderSelectorListenerAdapter {

        private volatile boolean leader = false;
        private volatile boolean disabled = false;
        private volatile Thread leaderThread;

        /** Timestamp of the last ZooKeeper-verified isLeader() call. */
        private long verifyTimestamp = 0L;

        /** Disable election participation (called on shutdown). */
        void disable() {
            disabled = true;
            setLeader(false);
            final Thread t = leaderThread;
            if (t != null) {
                t.interrupt();
            }
        }

        synchronized boolean isLeader() {
            if (!leader) {
                return false;
            }
            // Re-verify against ZooKeeper if the cache has expired.
            if (System.currentTimeMillis() - verifyTimestamp > VERIFY_CACHE_MS) {
                final boolean verified = verifyLeaderWithZk();
                setLeader(verified);
            }
            return leader;
        }

        private synchronized void setLeader(final boolean value) {
            this.leader = value;
            this.verifyTimestamp = System.currentTimeMillis();
        }

        @Override
        public synchronized void stateChanged(final CuratorFramework client,
                final ConnectionState newState) {
            LOGGER.info("ZooKeeper connection state changed to {} for node '{}'.", newState, nodeId);
            if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
                if (leader) {
                    LOGGER.warn("Node '{}' relinquishing leadership due to ZooKeeper connection state {}.", nodeId, newState);
                }
                setLeader(false);
            }
            super.stateChanged(client, newState);
        }

        /**
         * Curator calls this method on the node that has been elected leader.
         * We must block here for as long as we wish to remain leader. Returning
         * (or throwing) yields leadership back to the next candidate.
         */
        @Override
        public void takeLeadership(final CuratorFramework client) throws Exception {
            if (disabled) {
                return;
            }

            leaderThread = Thread.currentThread();
            setLeader(true);
            LOGGER.info("Node '{}' has been elected cluster leader.", nodeId);

            notifyStartLeading();

            // Verify with ZooKeeper periodically while we hold leadership.
            // Every 50 * 100 ms = 5 seconds, matching NiFi's pattern.
            try {
                int loopCount = 0;
                int failureCount = 0;
                while (!disabled && leader) {
                    try {
                        Thread.sleep(100L);
                    } catch (final InterruptedException ie) {
                        LOGGER.info("Node '{}' leader thread interrupted; relinquishing leadership.", nodeId);
                        Thread.currentThread().interrupt();
                        return;
                    }

                    if (++loopCount % 50 == 0) {
                        try {
                            if (!verifyLeaderWithZk()) {
                                LOGGER.info("ZooKeeper reports node '{}' is no longer the leader; relinquishing.", nodeId);
                                break;
                            }
                            failureCount = 0;
                        } catch (final Exception e) {
                            failureCount++;
                            if (failureCount > 1) {
                                LOGGER.warn("Node '{}' failed ZooKeeper leadership verification twice; relinquishing.", nodeId, e);
                                break;
                            } else {
                                LOGGER.warn("Node '{}' failed ZooKeeper leadership verification; will retry once.", nodeId, e);
                            }
                        }
                    }
                }
            } finally {
                setLeader(false);
                LOGGER.info("Node '{}' is no longer cluster leader.", nodeId);
                notifyStopLeading();
                leaderThread = null;
            }
        }
    }
}
