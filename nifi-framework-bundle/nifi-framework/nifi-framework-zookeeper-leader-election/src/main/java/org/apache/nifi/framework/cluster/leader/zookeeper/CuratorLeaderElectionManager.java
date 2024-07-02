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
package org.apache.nifi.framework.cluster.leader.zookeeper;

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
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.nifi.controller.leader.election.LeaderElectionStateChangeListener;
import org.apache.nifi.controller.leader.election.TrackedLeaderElectionManager;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.framework.cluster.zookeeper.ZooKeeperClientConfig;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class CuratorLeaderElectionManager extends TrackedLeaderElectionManager {
    private static final String OBSERVER_ID = "OBSERVER";

    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderElectionManager.class);

    private final ExecutorService leaderElectionMonitorEngine = new FlowEngine(4, CuratorLeaderElectionManager.class.getSimpleName());

    private final ZooKeeperClientConfig zkConfig;

    private CuratorFramework curatorClient;

    private volatile boolean stopped = true;

    private final ConcurrentMap<String, LeaderRole> leaderRoles = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, RegisteredRole> registeredRoles = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, String> lastKnownLeader = new ConcurrentHashMap<>();

    /**
     * Curator Leader Election Manager constructor with NiFi Properties for NarThreadContextClassLoader.createInstance()
     *
     * @param properties NiFi Properties
     */
    public CuratorLeaderElectionManager(final NiFiProperties properties) {
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
    public boolean isActiveParticipant(final String roleName) {
        final RegisteredRole role = registeredRoles.get(roleName);
        if (role == null) {
            return false;
        }

        final String participantId = role.getParticipantId();
        return participantId != null;
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

        final boolean participating = isParticipating(participantId);

        if (!isStopped()) {
            final ElectionListener electionListener = new ElectionListener(roleName, listener, participantId);
            final LeaderSelector leaderSelector = new LeaderSelector(curatorClient, leaderPath, leaderElectionMonitorEngine, electionListener);
            if (participating) {
                leaderSelector.autoRequeue();
                leaderSelector.setId(participantId);
                leaderSelector.start();
            }

            final LeaderRole leaderRole = new LeaderRole(leaderSelector, electionListener, participating);

            leaderRoles.put(roleName, leaderRole);
        }

        final String registeredId = participating ? participantId : OBSERVER_ID;
        logger.info("Registered for Election: Role [{}] Registered ID [{}]", roleName, registeredId);
    }

    @Override
    public synchronized void unregister(final String roleName) {
        registeredRoles.remove(roleName);

        final LeaderRole leaderRole = leaderRoles.remove(roleName);
        if (leaderRole == null) {
            logger.info("Cannot unregister Leader Election Role '{}' because that role is not registered", roleName);
            return;
        }

        final LeaderSelector leaderSelector = leaderRole.getLeaderSelector();
        if (leaderSelector == null) {
            logger.info("Cannot unregister Leader Election Role '{}' because that role is not registered", roleName);
            return;
        }

        leaderRole.getElectionListener().disable();

        try {
            leaderSelector.close();
        } catch (final Exception e) {
            // LeaderSelector will throw an IllegalStateException if it is not in the STARTED state.
            // However, it exposes no method to check its state, so we have to catch the exception and ignore it.
            logger.debug("Failed to close Leader Selector when unregistering for Role '{}'", roleName, e);
        }

        logger.info("Unregistered for Election: Role [{}]", roleName);
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

        leaderElectionMonitorEngine.shutdown();
        logger.info("{} stopped and closed", this);
    }

    private boolean isStopped() {
        return stopped;
    }

    @Override
    public String toString() {
        return "CuratorLeaderElectionManager[stopped=" + isStopped() + "]";
    }

    private LeaderRole getLeaderRole(final String roleName) {
        return leaderRoles.get(roleName);
    }

    @Override
    public boolean isLeader(final String roleName) {
        final boolean activeParticipant = isActiveParticipant(roleName);
        if (!activeParticipant) {
            logger.debug("Node is not an active participant in election for role {} so cannot be leader", roleName);
            return false;
        }

        final LeaderRole role = getLeaderRole(roleName);
        if (role == null) {
            logger.debug("Node is an active participant in election for role {} but there is no LeaderRole registered so this node cannot be leader", roleName);
            return false;
        }

        return role.isLeader();
    }

    @Override
    public Optional<String> getLeader(final String roleName) {
        if (isStopped()) {
            return determineLeaderExternal(roleName);
        }

        final LeaderRole role = getLeaderRole(roleName);
        if (role == null) {
            return determineLeaderExternal(roleName);
        }

        final long startNanos = System.nanoTime();
        try {

            Participant participant;
            try {
                participant = role.getLeaderSelector().getLeader();
            } catch (Exception e) {
                logger.warn("Unable to determine leader for role [{}]", roleName, e);
                return Optional.empty();
            }

            if (participant == null) {
                logger.debug("There is currently no elected leader for the {} role", roleName);
                return Optional.empty();
            }

            final String participantId = participant.getId();
            if (StringUtils.isEmpty(participantId)) {
                logger.debug("Found leader participant for role [{}] but the participantId was empty", roleName);
                return Optional.empty();
            }

            final String previousLeader = lastKnownLeader.put(roleName, participantId);
            if (previousLeader != null && !previousLeader.equals(participantId)) {
                onLeaderChanged(roleName);
            }

            return Optional.of(participantId);
        } finally {
            registerPollTime(System.nanoTime() - startNanos);
        }
    }

    /**
     * Use a new Curator client to determine which node is the elected leader for the given role.
     *
     * @param roleName the name of the role
     * @return the id of the elected leader, or <code>Optional.empty()</code> if no leader has been selected or if unable to determine
     *         the leader from ZooKeeper
     */
    private Optional<String> determineLeaderExternal(final String roleName) {
        final long start = System.nanoTime();

        try (CuratorFramework client = createClient()) {
            final LeaderSelectorListener electionListener = new LeaderSelectorListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                }

                @Override
                public void takeLeadership(CuratorFramework client) {
                }
            };

            final String electionPath = getElectionPath(roleName);

            // Note that we intentionally do not auto-requeue here, and we do not start the selector. We do not
            // want to join the leader election. We simply want to observe.
            final LeaderSelector selector = new LeaderSelector(client, electionPath, electionListener);

            try {
                final Participant leader = selector.getLeader();
                return leader == null ? Optional.empty() : Optional.of(leader.getId());
            } catch (final KeeperException.NoNodeException nne) {
                // If there is no ZNode, then there is no elected leader.
                return Optional.empty();
            } catch (final Exception e) {
                logger.warn("Unable to determine the Elected Leader for role '{}'; assuming no leader has been elected", roleName, e);
                return Optional.empty();
            }
        } finally {
            registerPollTime(System.nanoTime() - start);
        }
    }

    private CuratorFramework createClient() {
        // Create a new client because we don't want to try indefinitely for this to occur.
        final RetryPolicy retryPolicy = new RetryNTimes(1, 100);
        final CuratorACLProviderFactory aclProviderFactory = new CuratorACLProviderFactory();

        final CuratorFrameworkFactory.Builder clientBuilder = CuratorFrameworkFactory.builder()
            .connectString(zkConfig.getConnectString())
            .sessionTimeoutMs(zkConfig.getSessionTimeoutMillis())
            .connectionTimeoutMs(zkConfig.getConnectionTimeoutMillis())
            .ensembleTracker(zkConfig.isWithEnsembleTracker())
            .retryPolicy(retryPolicy)
            .aclProvider(aclProviderFactory.create(zkConfig))
            .defaultData(new byte[0]);

        if (zkConfig.isClientSecure()) {
            clientBuilder.zookeeperFactory(new SecureClientZooKeeperFactory(zkConfig));
        }

        final CuratorFramework client = clientBuilder.build();

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

        public ElectionListener getElectionListener() {
            return electionListener;
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
        private final String participantId;

        private volatile boolean leader;
        private volatile Thread leaderThread;
        private long leaderUpdateTimestamp = 0L;
        private final long MAX_CACHE_MILLIS = TimeUnit.SECONDS.toMillis(5L);

        public ElectionListener(final String roleName, final LeaderElectionStateChangeListener listener, final String participantId) {
            this.roleName = roleName;
            this.listener = listener;
            this.participantId = participantId;
        }

        public void disable() {
            logger.info("Election Listener for Role {} disabled", roleName);
            setLeader(false);

            if (leaderThread == null) {
                logger.debug("Election Listener for Role {} disabled but there is no leader thread. Will not interrupt any threads.", roleName);
            } else {
                leaderThread.interrupt();
            }
        }

        public synchronized boolean isLeader() {
            if (leaderUpdateTimestamp < System.currentTimeMillis() - MAX_CACHE_MILLIS) {
                try {
                    final long start = System.nanoTime();
                    final boolean zkLeader = verifyLeader();
                    final long nanos = System.nanoTime() - start;

                    setLeader(zkLeader);
                    logger.debug("Took {} nanoseconds to reach out to ZooKeeper in order to check whether or not this node is currently the leader for Role '{}'. ZooKeeper reported {}",
                        nanos, roleName, zkLeader);
                } catch (final Exception e) {
                    logger.warn("Attempted to reach out to ZooKeeper to determine whether or not this node is the elected leader for Role '{}' but failed to communicate with ZooKeeper. " +
                        "Assuming that this node is not the leader.", roleName, e);

                    return false;
                }
            } else {
                logger.debug("Checking if this node is leader for role {}: using cached response, returning {}", roleName, leader);
            }

            return leader;
        }

        private synchronized void setLeader(final boolean leader) {
            this.leader = leader;
            this.leaderUpdateTimestamp = System.currentTimeMillis();
        }

        @Override
        public synchronized void stateChanged(final CuratorFramework client, final ConnectionState newState) {
            logger.info("{} Connection State changed to {}", this, newState.name());

            if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
                if (leader) {
                    logger.info("Because Connection State was changed to {}, will relinquish leadership for role '{}'", newState, roleName);
                }

                setLeader(false);
            }

            super.stateChanged(client, newState);
        }

        /**
         * Reach out to ZooKeeper to verify that this node still is the leader. We do this because at times, a node will lose
         * its position as leader but the Curator client will fail to notify us, perhaps due to network failure, etc.
         *
         * @return <code>true</code> if this node is still the elected leader according to ZooKeeper, false otherwise
         */
        private boolean verifyLeader() {
            final Optional<String> leaderAddress = getLeader(roleName);
            if (!leaderAddress.isPresent()) {
                logger.debug("Reached out to ZooKeeper to determine which node is the elected leader for Role '{}' but found that there is no leader.", roleName);
                setLeader(false);
                return false;
            }

            final String leader = leaderAddress.get();
            final boolean match = leader.equals(participantId);
            logger.debug("Reached out to ZooKeeper to determine which node is the elected leader for Role '{}'. Elected Leader = '{}', Participant ID = '{}', This Node Elected = {}",
                roleName, leader, participantId, match);
            setLeader(match);
            return match;
        }

        @Override
        public void takeLeadership(final CuratorFramework client) throws Exception {
            leaderThread = Thread.currentThread();
            setLeader(true);
            logger.info("{} This node has been elected Leader for Role '{}'", this, roleName);

            if (listener != null) {
                try {
                    listener.onStartLeading();
                } catch (final Exception e) {
                    logger.error("This node was elected Leader for Role '{}' but failed to take leadership. Will relinquish leadership role. Failure was due to: {}", roleName, e);
                    setLeader(false);
                    Thread.sleep(1000L);
                    return;
                }
            }

            // Curator API states that we lose the leadership election when we return from this method,
            // so we will block as long as we are not interrupted or closed. Then, we will set leader to false.
            try {
                int failureCount = 0;
                int loopCount = 0;
                while (!isStopped() && leader) {
                    try {
                        Thread.sleep(100L);
                    } catch (final InterruptedException ie) {
                        logger.info("{} has been interrupted; no longer leader for role '{}'", this, roleName);
                        Thread.currentThread().interrupt();
                        return;
                    }

                    if (leader && ++loopCount % 50 == 0) {
                        // While Curator is supposed to interrupt this thread when we are no longer the leader, we have occasionally
                        // seen occurrences where the thread does not get interrupted. As a result, we will reach out to ZooKeeper
                        // periodically to determine whether or not this node is still the elected leader.
                        try {
                            final boolean stillLeader = verifyLeader();
                            failureCount = 0; // we got a response, so we were successful in communicating with zookeeper. Set failureCount back to 0.

                            if (!stillLeader) {
                                logger.info("According to ZooKeeper, this node is no longer the leader for Role '{}'. Will relinquish leadership.", roleName);
                                break;
                            }
                        } catch (final Exception e) {
                            failureCount++;
                            if (failureCount > 1) {
                                logger.warn("Attempted to reach out to ZooKeeper to verify that this node still is the elected leader for Role '{}' "
                                    + "but failed to communicate with ZooKeeper. This is the second failed attempt, so will relinquish leadership of this role.", roleName, e);
                            } else {
                                logger.warn("Attempted to reach out to ZooKeeper to verify that this node still is the elected leader for Role '{}' "
                                    + "but failed to communicate with ZooKeeper. Will wait a bit and attempt to communicate with ZooKeeper again before relinquishing this role.", roleName, e);
                            }
                        }
                    }
                }
            } finally {
                setLeader(false);
                logger.info("{} This node is no longer leader for role '{}'", this, roleName);

                if (listener != null) {
                    try {
                        listener.onStopLeading();
                    } catch (final Exception e) {
                        logger.error("This node is no longer leader for role '{}' but failed to shutdown leadership responsibilities properly", roleName, e);
                    }
                }
            }
        }
    }

    public static class SecureClientZooKeeperFactory implements ZookeeperFactory {

        public static final String NETTY_CLIENT_CNXN_SOCKET =
            org.apache.zookeeper.ClientCnxnSocketNetty.class.getName();

        private final ZKClientConfig zkSecureClientConfig;

        public SecureClientZooKeeperFactory(final ZooKeeperClientConfig zkConfig) {
            this.zkSecureClientConfig = new ZKClientConfig();

            // Netty is required for the secure client config.
            final String cnxnSocket = zkConfig.getConnectionSocket();
            if (!NETTY_CLIENT_CNXN_SOCKET.equals(cnxnSocket)) {
                throw new IllegalArgumentException(String.format("connection factory set to '%s', %s required", cnxnSocket, NETTY_CLIENT_CNXN_SOCKET));
            }
            zkSecureClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET, cnxnSocket);

            // This should never happen but won't get checked elsewhere.
            final boolean clientSecure = zkConfig.isClientSecure();
            if (!clientSecure) {
                throw new IllegalStateException(String.format("%s set to '%b', expected true", ZKClientConfig.SECURE_CLIENT, clientSecure));
            }
            zkSecureClientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, String.valueOf(clientSecure));

            final X509Util clientX509util = new ClientX509Util();
            zkSecureClientConfig.setProperty(clientX509util.getSslKeystoreLocationProperty(), zkConfig.getKeyStore());
            zkSecureClientConfig.setProperty(clientX509util.getSslKeystoreTypeProperty(), zkConfig.getKeyStoreType());
            zkSecureClientConfig.setProperty(clientX509util.getSslKeystorePasswdProperty(), zkConfig.getKeyStorePassword());
            zkSecureClientConfig.setProperty(clientX509util.getSslTruststoreLocationProperty(), zkConfig.getTrustStore());
            zkSecureClientConfig.setProperty(clientX509util.getSslTruststoreTypeProperty(), zkConfig.getTrustStoreType());
            zkSecureClientConfig.setProperty(clientX509util.getSslTruststorePasswdProperty(), zkConfig.getTrustStorePassword());
            zkSecureClientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER, Integer.toString(zkConfig.getJuteMaxbuffer()));
        }

        @Override
        public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
            return new ZooKeeperAdmin(connectString, sessionTimeout, watcher, zkSecureClientConfig);
        }
    }

}
