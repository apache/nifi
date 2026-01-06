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
import org.apache.nifi.security.cert.GeneralNameType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.InetAddress;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusteredLoadBalanceAuthorizerTest {
    private static final String LOCALHOST_ADDRESS = "localhost.local";

    private static final String WILDCARD_LOCAL_DOMAIN = "*.local";

    private static final String DNS_ADDRESS = "nifi.apache.org";

    @Mock
    private ClusterCoordinator clusterCoordinator;

    @Mock
    private SSLSocket sslSocket;

    @Mock
    private SSLSession sslSession;

    @Mock
    private X509Certificate peerCertificate;

    @Mock
    private NodeIdentifier nodeIdentifier;

    private ClusterLoadBalanceAuthorizer authorizer;

    @BeforeEach
    void setAuthorizer() {
        authorizer = new ClusterLoadBalanceAuthorizer(clusterCoordinator, EventReporter.NO_OP);
    }

    @Test
    void testAuthorizeNotAuthorized() throws IOException {
        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{peerCertificate});

        assertThrows(NotAuthorizedException.class, () -> authorizer.authorize(sslSocket));
    }

    @Test
    void testAuthorizeNodeIdentifierAuthorized() throws IOException, CertificateParsingException {
        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{peerCertificate});

        setPeerCertificate(LOCALHOST_ADDRESS);

        when(nodeIdentifier.getApiAddress()).thenReturn(LOCALHOST_ADDRESS);
        when(clusterCoordinator.getNodeIdentifiers()).thenReturn(Set.of(nodeIdentifier));

        final String authorized = authorizer.authorize(sslSocket);
        assertEquals(LOCALHOST_ADDRESS, authorized);
    }

    @Test
    void testAuthorizeWildcardAuthorized() throws IOException, CertificateParsingException {
        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{peerCertificate});
        final InetAddress socketAddress = InetAddress.getLocalHost();
        final String socketHostName = socketAddress.getHostName();
        when(sslSocket.getInetAddress()).thenReturn(socketAddress);

        setPeerCertificate(WILDCARD_LOCAL_DOMAIN);

        when(nodeIdentifier.getApiAddress()).thenReturn(LOCALHOST_ADDRESS);
        when(clusterCoordinator.getNodeIdentifiers()).thenReturn(Set.of(nodeIdentifier));

        final String authorized = authorizer.authorize(sslSocket);
        assertEquals(socketHostName, authorized);
    }

    @Test
    void testAuthorizeWildcardNotAuthorized() throws IOException, CertificateParsingException {
        when(sslSocket.getSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{peerCertificate});

        setPeerCertificate(WILDCARD_LOCAL_DOMAIN);

        when(nodeIdentifier.getApiAddress()).thenReturn(DNS_ADDRESS);
        when(clusterCoordinator.getNodeIdentifiers()).thenReturn(Set.of(nodeIdentifier));

        assertThrows(NotAuthorizedException.class, () -> authorizer.authorize(sslSocket));
    }

    private void setPeerCertificate(final String name) throws CertificateParsingException {
        final List<?> subjectAlternativeName = List.of(GeneralNameType.DNS_NAME.getNameType(), name);
        final Collection<List<?>> names = List.of(subjectAlternativeName);
        when(peerCertificate.getSubjectAlternativeNames()).thenReturn(names);
    }
}
