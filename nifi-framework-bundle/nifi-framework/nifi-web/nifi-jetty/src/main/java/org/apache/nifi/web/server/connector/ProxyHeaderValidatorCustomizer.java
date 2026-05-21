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
package org.apache.nifi.web.server.connector;

import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;

import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Set;

/**
 * Jetty Request Customizer implementing validation for supported Proxy Request Headers
 */
public class ProxyHeaderValidatorCustomizer implements HttpConfiguration.Customizer {
    private static final String MISDIRECTED_REQUEST_REASON = "Invalid Proxy Host Requested";

    private static final String REQUEST_REPLICATED_HEADER = "request-replicated";

    private static final Set<String> SUPPORTED_PROXY_HOST_HEADERS = Set.of(
            ProxyHeader.PROXY_HOST.getHeader(),
            ProxyHeader.FORWARDED_HOST.getHeader()
    );

    private final Set<String> validProxyHosts;

    public ProxyHeaderValidatorCustomizer(final Set<String> validProxyHosts) {
        this.validProxyHosts = Objects.requireNonNull(validProxyHosts, "Valid Proxy Hosts required");
    }

    /**
     * Validate requested proxy host header values against allowed proxy hosts and throw HTTP 421 on invalid values
     *
     * @param request HTTP Request to be evaluated
     * @param responseHeaders HTTP Response headers
     * @return Valid HTTP Request
     */
    @Override
    public Request customize(final Request request, final HttpFields.Mutable responseHeaders) {
        final Request customized;

        if (request.isSecure()) {
            customized = customizeSecureRequest(request);
        } else {
            customized = request;
        }

        return customized;
    }

    private Request customizeSecureRequest(final Request request) {
        final X509Certificate peerCertificate = findPeerCertificate(request);

        // Requests not authenticated with Client Certificates require header validation
        if (peerCertificate == null) {
            processProxyHostHeaders(request);
        } else {
            // Requests authenticated with Client Certificates but not indicated as replicated require header validation
            final HttpFields requestHeaders = request.getHeaders();
            final String requestReplicated = requestHeaders.get(REQUEST_REPLICATED_HEADER);
            if (requestReplicated == null) {
                processProxyHostHeaders(request);
            }
        }

        return request;
    }

    private void processProxyHostHeaders(final Request request) {
        final HttpURI requestUri = request.getHttpURI();
        final String requestHost = requestUri.getHost();

        final HttpFields requestHeaders = request.getHeaders();
        for (final String proxyHostHeader : SUPPORTED_PROXY_HOST_HEADERS) {
            final String hostHeader = requestHeaders.get(proxyHostHeader);
            // Include empty and blank values for enforced validation of request headers
            if (hostHeader == null) {
                continue;
            }
            // Allow proxy host header matching request host header based on TLS SNI and DNS SAN requirements
            if (requestHost.equals(hostHeader)) {
                continue;
            }
            if (validProxyHosts.contains(hostHeader)) {
                continue;
            }

            throw new HttpException.RuntimeException(HttpStatus.MISDIRECTED_REQUEST_421, MISDIRECTED_REQUEST_REASON);
        }
    }

    private X509Certificate findPeerCertificate(final Request request) {
        final X509Certificate peerCertificate;
        final ConnectionMetaData connectionMetaData = request.getConnectionMetaData();
        final Connection connection = connectionMetaData.getConnection();
        final EndPoint endPoint = connection.getEndPoint();
        final EndPoint.SslSessionData sslSessionData = endPoint.getSslSessionData();
        if (sslSessionData == null) {
            peerCertificate = null;
        } else {
            final X509Certificate[] peerCertificates = sslSessionData.peerCertificates();
            if (peerCertificates == null || peerCertificates.length == 0) {
                peerCertificate = null;
            } else {
                peerCertificate = peerCertificates[0];
            }
        }
        return peerCertificate;
    }
}
