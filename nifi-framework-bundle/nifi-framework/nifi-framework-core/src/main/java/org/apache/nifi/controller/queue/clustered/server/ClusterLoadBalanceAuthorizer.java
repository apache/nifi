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

package org.apache.nifi.controller.queue.clustered.server;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.security.cert.PeerIdentityProvider;
import org.apache.nifi.security.cert.StandardPeerIdentityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.Certificate;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

public class ClusterLoadBalanceAuthorizer implements LoadBalanceAuthorizer {
    private static final Logger logger = LoggerFactory.getLogger(ClusterLoadBalanceAuthorizer.class);

    private static final char WILDCARD = '*';

    private final PeerIdentityProvider peerIdentityProvider = new StandardPeerIdentityProvider();

    private final ClusterCoordinator clusterCoordinator;
    private final EventReporter eventReporter;

    public ClusterLoadBalanceAuthorizer(final ClusterCoordinator clusterCoordinator, final EventReporter eventReporter) {
        this.clusterCoordinator = clusterCoordinator;
        this.eventReporter = eventReporter;
    }

    @Override
    public String authorize(final SSLSocket sslSocket) throws IOException {
        final SSLSession sslSession = sslSocket.getSession();

        final Certificate[] peerCertificates = sslSession.getPeerCertificates();
        final Set<String> clientIdentities = peerIdentityProvider.getIdentities(peerCertificates);

        final Set<String> nodeIds = clusterCoordinator.getNodeIdentifiers().stream()
                .map(NodeIdentifier::getApiAddress)
                .collect(Collectors.toSet());

        logger.debug("Authorizing Peer {} against Cluster Nodes {}", clientIdentities, nodeIds);

        for (final String clientId : clientIdentities) {
            if (nodeIds.contains(clientId)) {
                logger.debug("Peer Certificate Identity [{}] authorized for Load Balancing from Cluster Node Identifiers", clientId);
                return clientId;
            }
        }

        for (final String nodeId : nodeIds) {
            if (isNodeIdMatched(nodeId, clientIdentities)) {
                final String clientId = sslSocket.getInetAddress().getHostName();
                logger.debug("Peer Socket Address [{}] for Node Identifier [{}] authorized for Load Balancing from Certificate Wildcard", clientId, nodeId);
                return clientId;
            }
        }

        final String message = "Peer Certificate Identities %s not authorized for Load Balancing".formatted(clientIdentities);
        logger.warn(message);
        eventReporter.reportEvent(Severity.WARNING, "Load Balanced Connections", message);
        throw new NotAuthorizedException(message);
    }

    private boolean isNodeIdMatched(final String nodeId, final Set<String> clientIdentities) {
        boolean matched = false;

        for (final String clientIdentity : clientIdentities) {
            final int wildcardIndex = clientIdentity.indexOf(WILDCARD);
            if (wildcardIndex == 0) {
                final String clientIdentityDomain = clientIdentity.substring(1);
                if (nodeId.endsWith(clientIdentityDomain)) {
                    matched = true;
                    break;
                }
            }
        }

        return matched;
    }
}
