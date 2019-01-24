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

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.security.util.CertificateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterLoadBalanceAuthorizer implements LoadBalanceAuthorizer {
    private static final Logger logger = LoggerFactory.getLogger(ClusterLoadBalanceAuthorizer.class);

    private final ClusterCoordinator clusterCoordinator;
    private final EventReporter eventReporter;
    private final HostnameVerifier hostnameVerifier;

    public ClusterLoadBalanceAuthorizer(final ClusterCoordinator clusterCoordinator, final EventReporter eventReporter) {
        this.clusterCoordinator = clusterCoordinator;
        this.eventReporter = eventReporter;
        this.hostnameVerifier = new DefaultHostnameVerifier();
    }

    @Override
    public String authorize(SSLSocket sslSocket) throws NotAuthorizedException, IOException {
        final SSLSession sslSession = sslSocket.getSession();

        final Set<String> clientIdentities;
        try {
            clientIdentities = getCertificateIdentities(sslSession);
        } catch (final CertificateException e) {
            throw new IOException("Failed to extract Client Certificate", e);
        }

        logger.debug("Will perform authorization against Client Identities '{}'", clientIdentities);

        final Set<String> nodeIds = clusterCoordinator.getNodeIdentifiers().stream()
                .map(NodeIdentifier::getApiAddress)
                .collect(Collectors.toSet());

        for (final String clientId : clientIdentities) {
            if (nodeIds.contains(clientId)) {
                logger.debug("Client ID '{}' is in the list of Nodes in the Cluster. Authorizing Client to Load Balance data", clientId);
                return clientId;
            }
        }

        // If there are no matches of Client IDs, try to verify it by HostnameVerifier. In this way, we can support wildcard certificates.
        for (final String nodeId : nodeIds) {
            if (hostnameVerifier.verify(nodeId, sslSession)) {
                final String clientId = sslSocket.getInetAddress().getHostName();
                logger.debug("The request was verified with node '{}'. The hostname derived from the socket is '{}'. Authorizing Client to Load Balance data", nodeId, clientId);
                return clientId;
            }
        }

        final String message = "Authorization failed for Client ID's to Load Balance data because none of the ID's are known Cluster Node Identifiers";

        logger.warn(message);
        eventReporter.reportEvent(Severity.WARNING, "Load Balanced Connections", message);
        throw new NotAuthorizedException("Client ID's " + clientIdentities + " are not authorized to Load Balance data");
    }

    private Set<String> getCertificateIdentities(final SSLSession sslSession) throws CertificateException, SSLPeerUnverifiedException {
        final Certificate[] certs = sslSession.getPeerCertificates();
        if (certs == null || certs.length == 0) {
            throw new SSLPeerUnverifiedException("No certificates found");
        }

        final X509Certificate cert = CertificateUtils.convertAbstractX509Certificate(certs[0]);
        cert.checkValidity();

        final Set<String> identities = CertificateUtils.getSubjectAlternativeNames(cert).stream()
                .map(CertificateUtils::extractUsername)
                .collect(Collectors.toSet());

        return identities;
    }
}
