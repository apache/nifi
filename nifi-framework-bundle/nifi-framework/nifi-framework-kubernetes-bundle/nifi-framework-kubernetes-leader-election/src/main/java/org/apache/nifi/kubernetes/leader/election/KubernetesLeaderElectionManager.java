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
package org.apache.nifi.kubernetes.leader.election;

import org.apache.nifi.controller.leader.election.LeaderElectionRole;
import org.apache.nifi.controller.leader.election.LeaderElectionStateChangeListener;
import org.apache.nifi.controller.leader.election.TrackedLeaderElectionManager;
import org.apache.nifi.kubernetes.client.KubernetesClientProvider;
import org.apache.nifi.kubernetes.client.NamespaceProvider;
import org.apache.nifi.kubernetes.client.ServiceAccountNamespaceProvider;
import org.apache.nifi.kubernetes.client.StandardKubernetesClientProvider;
import org.apache.nifi.kubernetes.leader.election.command.LeaderElectionCommandProvider;
import org.apache.nifi.kubernetes.leader.election.command.StandardLeaderElectionCommandProvider;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kubernetes Leader Election Manager implementation using Kubernetes Lease Resources
 */
public class KubernetesLeaderElectionManager extends TrackedLeaderElectionManager {
    private static final boolean INTERRUPT_ENABLED = true;

    private static final int SERVICE_THREADS = 4;

    private static final Logger logger = LoggerFactory.getLogger(KubernetesLeaderElectionManager.class);

    private static final Map<String, String> ROLE_NAMES;

    static {
        final Map<String, String> roleNames = new LinkedHashMap<>();
        for (final LeaderElectionRole leaderElectionRole : LeaderElectionRole.values()) {
            roleNames.put(leaderElectionRole.getRoleName(), leaderElectionRole.getRoleId());
        }
        ROLE_NAMES = Collections.unmodifiableMap(roleNames);
    }

    private final ExecutorService executorService;

    private final AtomicBoolean started = new AtomicBoolean();

    private final Map<String, Future<?>> roleCommands = new ConcurrentHashMap<>();

    private final Map<String, ParticipantRegistration> roleRegistrations = new ConcurrentHashMap<>();

    private final Map<String, String> roleLeaders = new ConcurrentHashMap<>();

    private final LeaderElectionCommandProvider leaderElectionCommandProvider;

    private final String roleIdPrefix;

    /**
     * Kubernetes Leader Election Manager constructor with NiFi Properties
     */
    public KubernetesLeaderElectionManager(final NiFiProperties nifiProperties) {
        final String leasePrefix = nifiProperties.getProperty(NiFiProperties.CLUSTER_LEADER_ELECTION_KUBERNETES_LEASE_PREFIX);
        this.roleIdPrefix = leasePrefix == null || leasePrefix.isBlank() ? null : leasePrefix;
        executorService = createExecutorService();
        leaderElectionCommandProvider = createLeaderElectionCommandProvider();
    }

    /**
     * Start Manager and register current roles
     */
    @Override
    public void start() {
        if (started.get()) {
            logger.debug("Start requested when running");
        } else {
            started.getAndSet(true);
            logger.debug("Started");

            for (final ParticipantRegistration roleRegistration : roleRegistrations.values()) {
                register(roleRegistration.roleName, roleRegistration.listener, roleRegistration.participantId);
            }
        }
    }

    /**
     * Stop Manager and shutdown running commands
     */
    @Override
    public void stop() {
        try {
            leaderElectionCommandProvider.close();
        } catch (final IOException e) {
            logger.warn("Leader Election Command Factory close failed", e);
        }
        roleLeaders.clear();
        executorService.shutdown();
        started.getAndSet(false);
        logger.debug("Stopped");
    }

    /**
     * Register for Election or Observation based on presence of Participant ID and register for Leader when started
     *
     * @param roleName Role Name for registration
     * @param listener State Change Listener for Leader Events
     * @param participantId Participant ID or null when registering for Observation
     */
    @Override
    public synchronized void register(final String roleName, final LeaderElectionStateChangeListener listener, final String participantId) {
        requireRoleName(roleName);
        Objects.requireNonNull(listener, "Change Listener required");

        final ParticipantRegistration roleRegistration = new ParticipantRegistration(roleName, participantId, listener);
        roleRegistrations.put(roleName, roleRegistration);

        final boolean participating = isParticipating(participantId);
        if (participating) {
            logger.debug("Registered Participation for Election Role [{}] ID [{}]", roleName, participantId);
            if (started.get()) {
                registerLeaderElectionCommand(roleName, listener, participantId);
            }
        } else {
            logger.info("Registered Observation for Election Role [{}]", roleName);
        }
    }

    /**
     * Unregister for Leader Election of specified Role and cancel running command
     *
     * @param roleName Role Name to be removed from registration
     */
    @Override
    public synchronized void unregister(final String roleName) {
        requireRoleName(roleName);

        roleLeaders.remove(roleName);

        final ParticipantRegistration roleRegistration = roleRegistrations.remove(roleName);
        if (roleRegistration == null) {
            logger.info("Not registered for Election Role [{}]", roleName);
        } else {
            final Future<?> roleCommand = roleCommands.remove(roleName);
            if (roleCommand == null) {
                logger.warn("Leader Election Command not found Role [{}] ID [{}]", roleName, roleRegistration.participantId);
            } else {
                roleCommand.cancel(INTERRUPT_ENABLED);
            }

            logger.info("Unregistered for Election Role [{}] ID [{}]", roleName, roleRegistration.participantId);
        }
    }

    /**
     * Determine whether current node is participating in Leader Election for specified Role
     *
     * @param roleName Role Name to be evaluated
     * @return Participation status in Leader Election
     */
    @Override
    public boolean isActiveParticipant(final String roleName) {
        requireRoleName(roleName);
        final String participantId = getParticipantId(roleName);
        return isParticipating(participantId);
    }

    /**
     * Get Leader Identifier for Role
     *
     * @param roleName Role Name for requested Leader Identifier
     * @return Leader Identifier or empty when not found
     */
    @Override
    public Optional<String> getLeader(final String roleName) {
        requireRoleName(roleName);

        final String roleId = getRoleId(roleName);

        final long pollStarted = System.nanoTime();
        try {
            final Optional<String> leader = leaderElectionCommandProvider.findLeader(roleId);
            leader.ifPresent(leaderId -> setRoleLeader(roleName, leaderId));
            return leader;
        } finally {
            final long elapsed = System.nanoTime() - pollStarted;
            registerPollTime(elapsed);
        }
    }

    /**
     * Determine whether current node is the Leader for the specified Role
     *
     * @param roleName Role Name to be evaluated
     * @return Leader Status
     */
    @Override
    public boolean isLeader(final String roleName) {
        requireRoleName(roleName);
        final boolean leader;

        final String participantId = getParticipantId(roleName);
        if (participantId == null) {
            logger.debug("Role [{}] not participating in Leader election", roleName);
            leader = false;
        } else {
            final Optional<String> leaderAddress = getLeader(roleName);
            final String leaderId = leaderAddress.orElse(null);
            leader = participantId.equals(leaderId);
            if (leader) {
                logger.debug("Role [{}] Participant ID [{}] is Leader", roleName, participantId);
            } else {
                logger.debug("Role [{}] Participant ID [{}] not Leader", roleName, leaderId);
            }
        }
        return leader;
    }

    protected ExecutorService createExecutorService() {
        return Executors.newFixedThreadPool(SERVICE_THREADS, new NamedThreadFactory());
    }

    protected LeaderElectionCommandProvider createLeaderElectionCommandProvider() {
        final NamespaceProvider namespaceProvider = new ServiceAccountNamespaceProvider();
        final String namespace = namespaceProvider.getNamespace();
        final KubernetesClientProvider kubernetesClientProvider = new StandardKubernetesClientProvider();
        return new StandardLeaderElectionCommandProvider(kubernetesClientProvider, namespace);
    }

    private synchronized void registerLeaderElectionCommand(final String roleName, final LeaderElectionStateChangeListener listener, final String participantId) {
        final Future<?> currentRoleCommand = roleCommands.get(roleName);
        if (currentRoleCommand == null) {
            final String roleId = getRoleId(roleName);
            final Runnable leaderElectionCommand = leaderElectionCommandProvider.getCommand(
                    roleId,
                    participantId,
                    listener::onStartLeading,
                    listener::onStopLeading,
                    leaderId -> setRoleLeader(roleName, leaderId)
            );

            final Future<?> roleCommand = executorService.submit(leaderElectionCommand);
            roleCommands.put(roleName, roleCommand);
            logger.info("Registered command for Election Role [{}] ID [{}]", roleName, participantId);
        }
    }

    private void setRoleLeader(final String roleName, final String leaderId) {
        final String previousLeaderId = roleLeaders.put(roleName, leaderId);
        if (leaderId.equals(previousLeaderId)) {
            logger.debug("Role [{}] Leader [{}] not changed", roleName, leaderId);
        } else {
            logger.debug("Role [{}] Leader [{}] Previous [{}] changed", roleName, leaderId, previousLeaderId);
            onLeaderChanged(roleName);
        }
    }

    private String getParticipantId(final String roleName) {
        final ParticipantRegistration roleRegistration = roleRegistrations.get(roleName);
        return roleRegistration == null ? null : roleRegistration.participantId;
    }

    private void requireRoleName(final String roleName) {
        if (roleName == null || roleName.isEmpty()) {
            throw new IllegalArgumentException("Role Name required");
        }
    }

    private String getRoleId(final String roleName) {
        final String roleId = ROLE_NAMES.get(roleName);
        if (roleId == null) {
            throw new IllegalArgumentException(String.format("Role Name [%s] not supported", roleName));
        }
        return roleIdPrefix == null ? roleId : String.format("%s-%s", roleIdPrefix, roleId);
    }

    private static class ParticipantRegistration {
        private final String roleName;

        private final String participantId;

        private final LeaderElectionStateChangeListener listener;

        private ParticipantRegistration(final String roleName, final String participantId, final LeaderElectionStateChangeListener listener) {
            this.roleName = roleName;
            this.participantId = participantId;
            this.listener = listener;
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = defaultFactory.newThread(runnable);
            thread.setName(KubernetesLeaderElectionManager.class.getSimpleName());
            thread.setDaemon(true);
            return thread;
        }
    }
}
