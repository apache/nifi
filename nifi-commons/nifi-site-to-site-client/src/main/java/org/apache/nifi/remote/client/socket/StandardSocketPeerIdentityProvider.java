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
package org.apache.nifi.remote.client.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.net.Socket;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;

/**
 * Standard implementation attempts to read X.509 certificates from an SSLSocket
 */
public class StandardSocketPeerIdentityProvider implements SocketPeerIdentityProvider {
    private static final Logger logger = LoggerFactory.getLogger(StandardSocketPeerIdentityProvider.class);

    @Override
    public Optional<String> getPeerIdentity(final Socket socket) {
        final Optional<String> peerIdentity;

        if (socket instanceof SSLSocket) {
            final SSLSocket sslSocket = (SSLSocket) socket;
            final SSLSession sslSession = sslSocket.getSession();
            peerIdentity = getPeerIdentity(sslSession);
        } else {
            peerIdentity = Optional.empty();
        }

        return peerIdentity;
    }

    private Optional<String> getPeerIdentity(final SSLSession sslSession) {
        String peerIdentity = null;

        final String peerHost = sslSession.getPeerHost();
        final int peerPort = sslSession.getPeerPort();

        try {
            final Certificate[] peerCertificates = sslSession.getPeerCertificates();
            if (peerCertificates == null || peerCertificates.length == 0) {
                logger.warn("Peer Identity not found: Peer Certificates not provided [{}:{}]", peerHost, peerPort);
            } else {
                final X509Certificate peerCertificate = (X509Certificate) peerCertificates[0];
                final Principal subjectDistinguishedName = peerCertificate.getSubjectDN();
                peerIdentity = subjectDistinguishedName.getName();
            }
        } catch (final SSLPeerUnverifiedException e) {
            logger.warn("Peer Identity not found: Peer Unverified [{}:{}]", peerHost, peerPort);
            logger.debug("TLS Protocol [{}] Peer Unverified [{}:{}]", sslSession.getProtocol(), peerHost, peerPort, e);
        }

        return Optional.ofNullable(peerIdentity);
    }
}
