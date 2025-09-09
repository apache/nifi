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
package org.apache.nifi.web.security.jwt.provider;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard Issuer Provider with configurable host and port for HTTPS URI construction
 */
public class StandardIssuerProvider implements IssuerProvider {
    private static final String HTTPS_SCHEME = "https";

    private static final Pattern HOST_CHARACTERS_PATTERN = Pattern.compile("[^a-zA-Z0-9-.]");

    private static final String REPLACEMENT_CHARACTER = "-";

    private final URI issuer;

    /**
     * Standard Issuer Provider constructor with optional host and required port properties
     *
     * @param host HTTPS Host address can be null or empty to default to InetAddress.getLocalHost() resolution
     * @param port HTTPS port number
     */
    public StandardIssuerProvider(final String host, final int port) {
        final String resolvedHost = getResolvedHost(host);
        try {
            this.issuer = new URI(HTTPS_SCHEME, null, resolvedHost, port, null, null, null);
        } catch (final URISyntaxException e) {
            throw new IllegalStateException("URI construction failed with Host [%s]".formatted(resolvedHost), e);
        }
    }

    /**
     * Get Issuer URI for issuer claims
     *
     * @return Issuer URI
     */
    @Override
    public URI getIssuer() {
        return issuer;
    }

    private String getResolvedHost(final String host) {
        final String resolvedHost;

        if (host == null || host.isEmpty()) {
            resolvedHost = getLocalHost();
        } else {
            resolvedHost = host;
        }

        final Matcher resolvedHostMatcher = HOST_CHARACTERS_PATTERN.matcher(resolvedHost);
        return resolvedHostMatcher.replaceAll(REPLACEMENT_CHARACTER);
    }

    private String getLocalHost() {
        try {
            final InetAddress localHostAddress = InetAddress.getLocalHost();
            return localHostAddress.getCanonicalHostName();
        } catch (final UnknownHostException e) {
            throw new IllegalStateException("Failed to resolve local host address", e);
        }
    }
}
