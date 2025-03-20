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

import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Set;

/**
 * Jetty Request Customizer implementing validation of port included in HTTP/1.1 Host Header or HTTP/2 authority header
 */
public class HostPortValidatorCustomizer implements HttpConfiguration.Customizer {
    private static final String MISDIRECTED_REQUEST_REASON = "Invalid Port Requested";

    private static final int PORT_NOT_SPECIFIED = -1;

    private final Set<Integer> validPorts;

    /**
     * HOst Port Validator Customer constructor with additional valid ports from application properties
     *
     * @param validPorts Valid Ports on HTTPS requests
     */
    public HostPortValidatorCustomizer(final Set<Integer> validPorts) {
        this.validPorts = Objects.requireNonNull(validPorts, "Valid Ports required");
    }

    /**
     * Validate requested port against connected port and valid ports for secure HTTPS requests.
     * The port is not specified when the header includes only the domain name as described in RFC 9110 Section 7.2.
     * The port must match the local socket address port or a configured valid port number.
     *
     * @param request HTTP Request to be evaluated
     * @param responseHeaders HTTP Response headers
     * @return Valid HTTP Request
     */
    @Override
    public Request customize(final Request request, final HttpFields.Mutable responseHeaders) {
        final Request customized;

        if (request.isSecure()) {
            final HttpURI requestUri = request.getHttpURI();
            final int port = requestUri.getPort();
            final int localSocketAddressPort = getLocalSocketAddressPort(request);

            if (PORT_NOT_SPECIFIED == port || localSocketAddressPort == port || validPorts.contains(port)) {
                customized = request;
            } else {
                throw new BadMessageException(HttpStatus.MISDIRECTED_REQUEST_421, MISDIRECTED_REQUEST_REASON);
            }
        } else {
            customized = request;
        }

        return customized;
    }

    private int getLocalSocketAddressPort(final Request request) {
        final ConnectionMetaData connectionMetaData = request.getConnectionMetaData();
        final InetSocketAddress localSocketAddress = (InetSocketAddress) connectionMetaData.getLocalSocketAddress();
        return localSocketAddress.getPort();
    }
}
