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
package org.apache.nifi.security.util;

import static java.util.Collections.unmodifiableSet;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.security.NoSuchAlgorithmException;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Transport Layer Security Platform provides runtime protocol configuration information
 */
public class TlsPlatform {
    private static final Pattern PROTOCOL_VERSION = Pattern.compile("TLSv(\\d+\\.?\\d*)");

    private static final int FIRST_GROUP = 1;

    private static final List<String> LEGACY_PROTOCOLS = Arrays.asList("TLSv1", "TLSv1.1");

    private static final SortedMap<Float, String> SORTED_PROTOCOLS = getDefaultSslContextProtocols();

    private static final Set<String> SUPPORTED_PROTOCOLS = unmodifiableSet(new TreeSet<>(SORTED_PROTOCOLS.values()).descendingSet());

    private static final Set<String> PREFERRED_PROTOCOLS = unmodifiableSet(
            SUPPORTED_PROTOCOLS.stream()
            .filter(protocol -> !LEGACY_PROTOCOLS.contains(protocol))
            .collect(Collectors.toSet())
    );

    /**
     * Get all supported protocols based on Java Security configuration
     *
     * @return Set of all supported Transport Layer Security Protocol names available on the JVM
     */
    public static Set<String> getSupportedProtocols() {
        return SUPPORTED_PROTOCOLS;
    }

    /**
     * Get Preferred Protocols based on supported protocols with legacy protocols removed
     *
     * @return Set of Preferred Transport Layer Security Protocol names
     */
    public static Set<String> getPreferredProtocols() {
        return PREFERRED_PROTOCOLS;
    }

    /**
     * Get Latest Protocol based on high version number from default protocols in Java Security configuration
     *
     * @return Latest Transport Layer Security Protocol
     */
    public static String getLatestProtocol() {
        return SORTED_PROTOCOLS.get(SORTED_PROTOCOLS.lastKey());
    }

    private static SortedMap<Float, String> getDefaultSslContextProtocols() {
        final SSLContext defaultContext = getDefaultSslContext();
        final SSLParameters sslParameters = defaultContext.getSupportedSSLParameters();
        final String[] protocols = sslParameters.getProtocols();

        final SortedMap<Float, String> sslContextProtocols = new TreeMap<>();
        for (final String protocol : protocols) {
            final Matcher protocolVersionMatcher = PROTOCOL_VERSION.matcher(protocol);
            if (protocolVersionMatcher.matches()) {
                final String protocolVersion = protocolVersionMatcher.group(FIRST_GROUP);
                final float version = Float.parseFloat(protocolVersion);
                sslContextProtocols.put(version, protocol);
            }
        }

        return sslContextProtocols;
    }

    private static SSLContext getDefaultSslContext() {
        try {
            return SSLContext.getDefault();
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("SSLContext.getDefault() Failed", e);
        }
    }
}
