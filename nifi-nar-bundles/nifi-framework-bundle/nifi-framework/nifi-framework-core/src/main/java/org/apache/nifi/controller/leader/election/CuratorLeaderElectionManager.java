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
import org.apache.zookeeper.KeeperException;
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

        curatorClient = createClient();

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
    public void register(String roleName, LeaderElectionStateChangeListener listener) {
        register(roleName, listener, null);
    }

    private String getElectionPath(final String roleName) {
        final String rootPath = zkConfig.getRootPath();
        final String leaderPath = rootPath + (rootPath.endsWith("/") ? "" : "/") + "leaders/" + roleName;
        return leaderPath;
    }

    @Override
    public synchronized void register(final String roleName, final LeaderElectionStateChangeListener listener, final String participantId) {
        logger.debug("{} Registering new Leader Selector for role {}", this, roleName);

        // If we already have a Leader Role registered and either the Leader Role is participating in election,
        // or the given participant id == null (don't want to participant in election) then we're done.
        final LeaderRole currentRole = leaderRoles.get(roleName);
        if (currentRole != null && (currentRole.isParticipant() || participantId == null)) {
            logger.info("{} Attempted to register Leader Election for role '{}' but this role is already registered", this, roleName);
            return;
        }

        final String leaderPath = getElectionPath(roleName);

        try {
            PathUtils.validatePath(leaderPath);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException("Cannot register leader election for role '" + roleName + "' because this is not a valid role name");
        }

        registeredRoles.put(roleName, new RegisteredRole(participantId, listener));

        final boolean isParticipant = participantId != null && !participantId.trim().isEmpty();

        if (!isStopped()) {
            final ElectionListener electionListener = new ElectionListener(roleName, listener);
            final LeaderSelector leaderSelector = new LeaderSelector(curatorClient, leaderPath, leaderElectionMonitorEngine, electionListener);
            if (isParticipant) {
                leaderSelector.autoRequeue();
                leaderSelector.setId(participantId);
                leaderSelector.start();
            }

            final LeaderRole leaderRole = new LeaderRole(leaderSelector, electionListener, isParticipant);

            leaderRoles.put(roleName, leaderRole);
        }

        if (isParticipant) {
            logger.info("{} Registered new Leader Selector for role {}; this node is an active participant in the election.", this, roleName);
        } else {
            logger.info("{} Registered new Leader Selector for role {}; this node is a silent observer in the election.", this, roleName);
        }
    }

    @Override
    public synchronized void unregister(final String roleName) {
        registeredRoles.remove(roleName);

        final LeaderRole leaderRole = leaderRoles.remove(roleName);
        if (leaderRole == null) {
            logger.info("Cannot unregister Leader Election Role '{}' becuase that role is not registered", roleName);
            return;
        }

        final LeaderSelector leaderSelector = leaderRole.getLeaderSelector();
        if (leaderSelector == null) {
            logger.info("Cannot unregister Leader Election Role '{}' becuase that role is not registered", roleName);
            return;
        }

        leaderSelector.close();
        logger.info("This node is no longer registered to be elected as the Leader for Role '{}'", roleName);
    }

    @Override
    public synchronized void stop() {
        stopped = true;

        for (final Map.Entry<String, LeaderRole> entry : leaderRoles.entrySet()) {
            final LeaderRole role = entry.getValue();
            final LeaderSelector selector = role.getLeaderSelector();

            try {
                selector.close();
            } catch (final Exception e) {
                logger.warn("Failed to close Leader Selector for {}", entry.getKey(), e);
            }
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
        if (isStopped()) {
            return determineLeaderExternal(roleName);
        }

        final LeaderRole role = getLeaderRole(roleName);
        if (role == null) {
            return determineLeaderExternal(roleName);
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


    /**
     * Determines whether or not leader election has already begun for the role with the given name
     *
     * @param roleName the role of interest
     * @return <code>true</code> if leader election has already begun, <code>false</code> if it has not or if unable to determine this.
     */
    @Override
    public boolean isLeaderElected(final String roleName) {
        final String leaderAddress = determineLeaderExternal(roleName);
        return !StringUtils.isEmpty(leaderAddress);
    }


    /**
     * Use a new Curator client to determine which node is the elected leader for the given role.
     *
     * @param roleName the name of the role
     * @return the id of the elected leader, or <code>null</code> if no leader has been selected or if unable to determine
     *         the leader from ZooKeeper
     */
    private String determineLeaderExternal(final String roleName) {
        final CuratorFramework client = createClient();
        try {
            final LeaderSelectorListener electionListener = new LeaderSelectorListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                }

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception {
                }
            };

            final String electionPath = getElectionPath(roleName);

            // Note that we intentionally do not auto-requeue here, and we do not start the selector. We do not
            // want to join the leader election. We simply want to observe.
            final LeaderSelector selector = new LeaderSelector(client, electionPath, electionListener);

            try {
                final Participant leader = selector.getLeader();
                return leader == null ? null : leader.getId();
            } catch (final KeeperException.NoNodeException nne) {
                // If there is no ZNode, then there is no elected leader.
                return null;
            } catch (final Exception e) {
                logger.warn("Unable to determine the Elected Leader for role '{}' due to {}; assuming no leader has been elected", roleName, e.toString());
                if (logger.isDebugEnabled()) {
                    logger.warn("", e);
                }

                return null;
            }
        } finally {
            client.close();
        }
    }

    private CuratorFramework createClient() {
        // Create a new client because we don't want to try indefinitely for this to occur.
        final RetryPolicy retryPolicy = new RetryNTimes(1, 100);
        final CuratorACLProviderFactory aclProviderFactory = new CuratorACLProviderFactory();

        final CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkConfig.getConnectString())
            .sessionTimeoutMs(zkConfig.getSessionTimeoutMillis())
            .connectionTimeoutMs(zkConfig.getConnectionTimeoutMillis())
            .retryPolicy(retryPolicy)
            .aclProvider(aclProviderFactory.create(zkConfig))
            .defaultData(new byte[0])
            .build();

        client.start();
        return client;
    }


    private static class LeaderRole {

        private final LeaderSelector leaderSelector;
        private final ElectionListener electionListener;
        private final boolean participant;

        public LeaderRole(final LeaderSelector leaderSelector, final ElectionListener electionListener, final boolean participant) {
            this.leaderSelector = leaderSelector;
            this.electionListener = electionListener;
            this.participant = participant;
        }

        public LeaderSelector getLeaderSelector() {
            return leaderSelector;
        }

        public boolean isLeader() {
            return electionListener.isLeader();
        }

        public boolean isParticipant() {
            return participant;
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
