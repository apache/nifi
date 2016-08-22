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
package org.apache.nifi.controller.leader.election;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorLeaderElectionManager implements LeaderElectionManager {

    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderElectionManager.class);

    private final FlowEngine leaderElectionMonitorEngine;
    private final ZooKeeperClientConfig zkConfig;

    private CuratorFramework curatorClient;

    private volatile boolean stopped = true;

    private final Map<String, LeaderRole> leaderRoles = new HashMap<>();
    private final Map<String, RegisteredRole> registeredRoles = new HashMap<>();

    public CuratorLeaderElectionManager(final int threadPoolSize, final NiFiProperties properties) {
        leaderElectionMonitorEngine = new FlowEngine(threadPoolSize, "Leader Election Notification", true);
        zkConfig = ZooKeeperClientConfig.createConfig(properties);
    }

    @Override
    public synchronized void start() {
        if (!stopped) {
            return;
        }

        stopped = false;

        final RetryPolicy retryPolicy = new RetryNTimes(1, 100);
        curatorClient = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getConnectString())
                .sessionTimeoutMs(zkConfig.getSessionTimeoutMillis())
                .connectionTimeoutMs(zkConfig.getConnectionTimeoutMillis())
                .retryPolicy(retryPolicy)
                .defaultData(new byte[0])
                .build();

        curatorClient.start();

        // Call #register for each already-registered role. This will
        // cause us to start listening for leader elections for that
        // role again
        for (final Map.Entry<String, RegisteredRole> entry : registeredRoles.entrySet()) {
            final RegisteredRole role = entry.getValue();
            register(entry.getKey(), role.getListener(), role.getParticipantId());
        }

        logger.info("{} started", this);
    }

    @Override
    public synchronized void register(final String roleName) {
        register(roleName, null);
    }

    @Override
    public void register(String roleName, LeaderElectionStateChangeListener listener) {
        register(roleName, listener, null);
    }

    @Override
    public synchronized void register(final String roleName, final LeaderElectionStateChangeListener listener, final String participantId) {
        logger.debug("{} Registering new Leader Selector for role {}", this, roleName);

        if (leaderRoles.containsKey(roleName)) {
            logger.info("{} Attempted to register Leader Election for role '{}' but this role is already registered", this, roleName);
            return;
        }

        final String rootPath = zkConfig.getRootPath();
        final String leaderPath = rootPath + (rootPath.endsWith("/") ? "" : "/") + "leaders/" + roleName;

        try {
            PathUtils.validatePath(rootPath);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException("Cannot register leader election for role '" + roleName + "' because this is not a valid role name");
        }

        registeredRoles.put(roleName, new RegisteredRole(participantId, listener));

        if (!isStopped()) {
            final ElectionListener electionListener = new ElectionListener(roleName, listener);
            final LeaderSelector leaderSelector = new LeaderSelector(curatorClient, leaderPath, leaderElectionMonitorEngine, electionListener);
            leaderSelector.autoRequeue();
            if (participantId != null) {
                leaderSelector.setId(participantId);
            }

            leaderSelector.start();

            final LeaderRole leaderRole = new LeaderRole(leaderSelector, electionListener);

            leaderRoles.put(roleName, leaderRole);
        }

        logger.info("{} Registered new Leader Selector for role {}", this, roleName);
    }

    @Override
    public synchronized void unregister(final String roleName) {
        registeredRoles.remove(roleName);

        final LeaderRole leaderRole = leaderRoles.remove(roleName);
        final LeaderSelector leaderSelector = leaderRole.getLeaderSelector();
        if (leaderSelector == null) {
            logger.warn("Cannot unregister Leader Election Role '{}' becuase that role is not registered", roleName);
            return;
        }

        leaderSelector.close();
        logger.info("This node is no longer registered to be elected as the Leader for Role '{}'", roleName);
    }

    @Override
    public synchronized void stop() {
        stopped = true;

        for (final LeaderRole role : leaderRoles.values()) {
            final LeaderSelector selector = role.getLeaderSelector();
            selector.close();
        }

        leaderRoles.clear();

        if (curatorClient != null) {
            curatorClient.close();
            curatorClient = null;
        }

        logger.info("{} stopped and closed", this);
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public String toString() {
        return "CuratorLeaderElectionManager[stopped=" + isStopped() + "]";
    }

    private synchronized LeaderRole getLeaderRole(final String roleName) {
        return leaderRoles.get(roleName);
    }

    @Override
    public boolean isLeader(final String roleName) {
        final LeaderRole role = getLeaderRole(roleName);
        if (role == null) {
            return false;
        }

        return role.isLeader();
    }

    @Override
    public String getLeader(final String roleName) {
        final LeaderRole role = getLeaderRole(roleName);
        if (role == null) {
            return null;
        }

        Participant participant;
        try {
            participant = role.getLeaderSelector().getLeader();
        } catch (Exception e) {
            logger.debug("Unable to determine leader for role '{}'; returning null", roleName);
            return null;
        }

        if (participant == null) {
            return null;
        }

        final String participantId = participant.getId();
        if (StringUtils.isEmpty(participantId)) {
            return null;
        }

        return participantId;
    }

    private static class LeaderRole {

        private final LeaderSelector leaderSelector;
        private final ElectionListener electionListener;

        public LeaderRole(final LeaderSelector leaderSelector, final ElectionListener electionListener) {
            this.leaderSelector = leaderSelector;
            this.electionListener = electionListener;
        }

        public LeaderSelector getLeaderSelector() {
            return leaderSelector;
        }

        public boolean isLeader() {
            return electionListener.isLeader();
        }
    }

    private static class RegisteredRole {

        private final LeaderElectionStateChangeListener listener;
        private final String participantId;

        public RegisteredRole(final String participantId, final LeaderElectionStateChangeListener listener) {
            this.participantId = participantId;
            this.listener = listener;
        }

        public LeaderElectionStateChangeListener getListener() {
            return listener;
        }

        public String getParticipantId() {
            return participantId;
        }
    }

    private class ElectionListener extends LeaderSelectorListenerAdapter implements LeaderSelectorListener {

        private final String roleName;
        private final LeaderElectionStateChangeListener listener;

        private volatile boolean leader;

        public ElectionListener(final String roleName, final LeaderElectionStateChangeListener listener) {
            this.roleName = roleName;
            this.listener = listener;
        }

        public boolean isLeader() {
            return leader;
        }

        @Override
        public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
            logger.info("{} Connection State changed to {}", this, newState.name());
            super.stateChanged(client, newState);
        }

        @Override
        public void takeLeadership(final CuratorFramework client) throws Exception {
            leader = true;
            logger.info("{} This node has been elected Leader for Role '{}'", this, roleName);

            if (listener != null) {
                try {
                    listener.onLeaderElection();
                } catch (final Exception e) {
                    logger.error("This node was elected Leader for Role '{}' but failed to take leadership. Will relinquish leadership role. Failure was due to: {}", roleName, e);
                    logger.error("", e);
                    leader = false;
                    Thread.sleep(1000L);
                    return;
                }
            }

            // Curator API states that we lose the leadership election when we return from this method,
            // so we will block as long as we are not interrupted or closed. Then, we will set leader to false.
            try {
                while (!isStopped()) {
                    try {
                        Thread.sleep(100L);
                    } catch (final InterruptedException ie) {
                        logger.info("{} has been interrupted; no longer leader for role '{}'", this, roleName);
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } finally {
                leader = false;
                logger.info("{} This node is no longer leader for role '{}'", this, roleName);

                if (listener != null) {
                    try {
                        listener.onLeaderRelinquish();
                    } catch (final Exception e) {
                        logger.error("This node is no longer leader for role '{}' but failed to shutdown leadership responsibilities properly due to: {}", roleName, e.toString());
                        if (logger.isDebugEnabled()) {
                            logger.error("", e);
                        }
                    }
                }
            }
        }
    }
}
