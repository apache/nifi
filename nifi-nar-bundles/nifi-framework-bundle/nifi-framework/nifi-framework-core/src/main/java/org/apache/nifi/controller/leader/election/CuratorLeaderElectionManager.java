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
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorLeaderElectionManager implements LeaderElectionManager {
    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderElectionManager.class);

    private final FlowEngine leaderElectionMonitorEngine;
    private final int sessionTimeoutMs;
    private final int connectionTimeoutMs;
    private final String rootPath;
    private final String connectString;

    private CuratorFramework curatorClient;

    private volatile boolean stopped = true;

    private final Map<String, LeaderRole> leaderRoles = new HashMap<>();
    private final Map<String, LeaderElectionStateChangeListener> registeredRoles = new HashMap<>();

    public CuratorLeaderElectionManager(final int threadPoolSize) {
        leaderElectionMonitorEngine = new FlowEngine(threadPoolSize, "Leader Election Notification", true);

        final NiFiProperties properties = NiFiProperties.getInstance();

        connectString = properties.getProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING);
        if (connectString == null || connectString.trim().isEmpty()) {
            throw new IllegalStateException("The '" + NiFiProperties.ZOOKEEPER_CONNECT_STRING + "' property is not set in nifi.properties");
        }

        sessionTimeoutMs = getTimePeriod(properties, NiFiProperties.ZOOKEEPER_SESSION_TIMEOUT, NiFiProperties.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
        connectionTimeoutMs = getTimePeriod(properties, NiFiProperties.ZOOKEEPER_CONNECT_TIMEOUT, NiFiProperties.DEFAULT_ZOOKEEPER_CONNECT_TIMEOUT);
        rootPath = properties.getProperty(NiFiProperties.ZOOKEEPER_ROOT_NODE, NiFiProperties.DEFAULT_ZOOKEEPER_ROOT_NODE);

        try {
            PathUtils.validatePath(rootPath);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException("The '" + NiFiProperties.ZOOKEEPER_ROOT_NODE + "' property in nifi.properties is set to an illegal value: " + rootPath);
        }
    }


    @Override
    public synchronized void start() {
        if (!stopped) {
            return;
        }

        stopped = false;

        final RetryPolicy retryPolicy = new RetryForever(5000);
        curatorClient = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        curatorClient.start();

        // Call #register for each already-registered role. This will
        // cause us to start listening for leader elections for that
        // role again
        for (final Map.Entry<String, LeaderElectionStateChangeListener> entry : registeredRoles.entrySet()) {
            register(entry.getKey(), entry.getValue());
        }

        logger.info("{} started", this);
    }

    private int getTimePeriod(final NiFiProperties properties, final String propertyName, final String defaultValue) {
        final String timeout = properties.getProperty(propertyName, defaultValue);
        try {
            return (int) FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Value of '" + propertyName + "' property is set to '" + timeout + "', which is not a valid time period. Using default of " + defaultValue);
            return (int) FormatUtils.getTimeDuration(defaultValue, TimeUnit.MILLISECONDS);
        }
    }


    @Override
    public synchronized void register(final String roleName) {
        register(roleName, null);
    }


    @Override
    public synchronized void register(final String roleName, final LeaderElectionStateChangeListener listener) {
        logger.debug("{} Registering new Leader Selector for role {}", this, roleName);

        if (leaderRoles.containsKey(roleName)) {
            logger.warn("{} Attempted to register Leader Election for role '{}' but this role is already registered", this, roleName);
            return;
        }

        final String leaderPath = (rootPath.endsWith("/") ? "" : "/") + "leaders/" + roleName;

        try {
            PathUtils.validatePath(rootPath);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException("Cannot register leader election for role '" + roleName + "' because this is not a valid role name");
        }

        registeredRoles.put(roleName, listener);

        if (!isStopped()) {
            final ElectionListener electionListener = new ElectionListener(roleName, listener);
            final LeaderSelector leaderSelector = new LeaderSelector(curatorClient, leaderPath, leaderElectionMonitorEngine, electionListener);
            leaderSelector.autoRequeue();
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


    @Override
    public synchronized boolean isLeader(final String roleName) {
        final LeaderRole role = leaderRoles.get(roleName);
        if (role == null) {
            return false;
        }

        return role.isLeader();
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
                leaderElectionMonitorEngine.submit(new Runnable() {
                    @Override
                    public void run() {
                        listener.onLeaderElection();
                    }
                });
            }

            // Curator API states that we lose the leadership election when we return from this method,
            // so we will block as long as we are not interrupted or closed. Then, we will set leader to false.
            try {
                while (!isStopped()) {
                    try {
                        Thread.sleep(1000L);
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
                    leaderElectionMonitorEngine.submit(new Runnable() {
                        @Override
                        public void run() {
                            listener.onLeaderRelinquish();
                        }
                    });
                }
            }
        }
    }
}
