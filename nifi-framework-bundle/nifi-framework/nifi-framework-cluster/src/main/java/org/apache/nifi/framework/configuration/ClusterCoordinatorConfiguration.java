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
package org.apache.nifi.framework.configuration;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.flow.FlowElection;
import org.apache.nifi.cluster.coordination.flow.PopularVoteFlowElection;
import org.apache.nifi.cluster.coordination.heartbeat.ClusterProtocolHeartbeatMonitor;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.coordination.node.NodeClusterCoordinator;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.firewall.impl.FileBasedClusterNodeFirewall;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

/**
 * Cluster Coordinator Configuration
 */
@Configuration
public class ClusterCoordinatorConfiguration {
    private NiFiProperties properties;

    private ClusterCoordinationProtocolSenderListener protocolListener;

    private NodeProtocolSender nodeProtocolSender;

    private RevisionManager revisionManager;

    private EventReporter eventReporter;

    private LeaderElectionManager leaderElectionManager;

    private StateManagerProvider stateManagerProvider;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void setStateManagerProvider(final StateManagerProvider stateManagerProvider) {
        this.stateManagerProvider = stateManagerProvider;
    }

    @Autowired
    public void setLeaderElectionManager(final LeaderElectionManager leaderElectionManager) {
        this.leaderElectionManager = leaderElectionManager;
    }

    @Autowired
    public void setEventReporter(final EventReporter eventReporter) {
        this.eventReporter = eventReporter;
    }

    @Autowired
    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    @Qualifier("nodeProtocolSender")
    @Autowired(required = false)
    public void setNodeProtocolSender(final NodeProtocolSender nodeProtocolSender) {
        this.nodeProtocolSender = nodeProtocolSender;
    }

    @Autowired(required = false)
    public void setProtocolListener(final ClusterCoordinationProtocolSenderListener protocolListener) {
        this.protocolListener = protocolListener;
    }

    @Bean
    public ClusterCoordinator clusterCoordinator() {
        final ClusterCoordinator clusterCoordinator;

        if (properties.isClustered()) {
            try {
                clusterCoordinator = new NodeClusterCoordinator(
                        protocolListener,
                        eventReporter,
                        leaderElectionManager,
                        flowElection(),
                        clusterFirewall(),
                        revisionManager,
                        properties,
                        nodeProtocolSender,
                        stateManagerProvider
                );
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to load Cluster Coordinator", e);
            }
        } else {
            clusterCoordinator = null;
        }

        return clusterCoordinator;
    }

    @Bean
    public HeartbeatMonitor heartbeatMonitor() {
        final HeartbeatMonitor heartbeatMonitor;

        final ClusterCoordinator clusterCoordinator = clusterCoordinator();
        if (clusterCoordinator == null) {
            heartbeatMonitor = null;
        } else {
            heartbeatMonitor = new ClusterProtocolHeartbeatMonitor(clusterCoordinator, protocolListener, properties);
        }

        return heartbeatMonitor;
    }

    private ClusterNodeFirewall clusterFirewall() {
        final ClusterNodeFirewall firewall;

        final File config = properties.getClusterNodeFirewallFile();
        final File restoreDirectory = properties.getRestoreDirectory();
        if (config == null) {
            firewall = null;
        } else {
            try {
                firewall = new FileBasedClusterNodeFirewall(config, restoreDirectory);
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to load Cluster Firewall configuration", e);
            }
        }

        return firewall;
    }

    private FlowElection flowElection() {
        final String maxWaitTime = properties.getFlowElectionMaxWaitTime();
        final long maxWaitMillis = FormatUtils.getTimeDuration(maxWaitTime, TimeUnit.MILLISECONDS);

        final Integer maxNodes = properties.getFlowElectionMaxCandidates();
        return new PopularVoteFlowElection(maxWaitMillis, TimeUnit.MILLISECONDS, maxNodes);
    }
}
