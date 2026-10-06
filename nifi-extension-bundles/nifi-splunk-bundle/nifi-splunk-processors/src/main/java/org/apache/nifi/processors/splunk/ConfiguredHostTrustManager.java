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
package org.apache.nifi.processors.splunk;

import org.apache.nifi.logging.ComponentLog;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Trust manager that skips certificate subject name matching when the TLS peer host is the hostname
 * configured on the processor, and delegates certificate path validation. Any other peer is checked
 * by the delegate trust manager.
 */
class ConfiguredHostTrustManager extends X509ExtendedTrustManager {
    private final X509TrustManager delegate;
    private final String configuredHost;
    private final ComponentLog logger;

    ConfiguredHostTrustManager(final X509TrustManager delegate, final String hostname, final ComponentLog logger) {
        this.delegate = Objects.requireNonNull(delegate, "Trust Manager required");
        this.logger = Objects.requireNonNull(logger, "Logger required");
        final String requiredHostname = Objects.requireNonNull(hostname, "Hostname required");
        if (requiredHostname.isBlank()) {
            throw new IllegalArgumentException("Hostname required");
        }

        this.configuredHost = requiredHostname;
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        // Callers that do not supply a peer socket or engine cannot be matched to the configured hostname.
        delegate.checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType, final Socket socket) throws CertificateException {
        if (delegate instanceof final X509ExtendedTrustManager extendedTrustManager) {
            extendedTrustManager.checkClientTrusted(chain, authType, socket);
        } else {
            delegate.checkClientTrusted(chain, authType);
        }
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType, final Socket socket) throws CertificateException {
        if (socket instanceof final SSLSocket sslSocket) {
            final SSLSession session = sslSocket.getHandshakeSession();
            if (session == null) {
                throw new CertificateException("No handshake session");
            }

            final String peerHost = session.getPeerHost();
            if (configuredHost.contentEquals(peerHost)) {
                logger.debug("Peer Host [{}] matches Configured Host [{}]", peerHost, configuredHost);
                delegate.checkServerTrusted(chain, authType);
                return;
            }
        }

        if (delegate instanceof final X509ExtendedTrustManager extendedTrustManager) {
            extendedTrustManager.checkServerTrusted(chain, authType, socket);
        } else {
            delegate.checkServerTrusted(chain, authType);
        }
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine) throws CertificateException {
        if (delegate instanceof final X509ExtendedTrustManager extendedTrustManager) {
            extendedTrustManager.checkClientTrusted(chain, authType, engine);
        } else {
            delegate.checkClientTrusted(chain, authType);
        }
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine) throws CertificateException {
        if (delegate instanceof final X509ExtendedTrustManager extendedTrustManager) {
            try {
                extendedTrustManager.checkServerTrusted(chain, authType, engine);
            } catch (final CertificateException e) {
                final String peerHost = engine == null ? null : engine.getPeerHost();
                if (peerHost == null) {
                    // Throw CertificateException when no further evaluation is possible based on lack of peer host address
                    throw e;
                } else {
                    if (configuredHost.contentEquals(peerHost)) {
                        logger.debug("Peer Host [{}] matches Configured Host [{}]", peerHost, configuredHost);
                    } else {
                        throw new CertificateException("Peer Host [%s] does not match Configured Host [%s]".formatted(peerHost, configuredHost), e);
                    }
                }
            }
        } else {
            delegate.checkServerTrusted(chain, authType);
        }
    }
}
