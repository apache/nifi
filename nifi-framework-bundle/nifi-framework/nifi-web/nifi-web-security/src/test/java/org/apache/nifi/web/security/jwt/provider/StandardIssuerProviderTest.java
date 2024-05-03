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

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class StandardIssuerProviderTest {
    private static final String HTTPS_SCHEME = "https";

    private static final String LOCALHOST = "localhost.localdomain";

    private static final int PORT = 8443;

    private static final String EMPTY = "";

    @Test
    void testGetIssuer() {
        final StandardIssuerProvider provider = new StandardIssuerProvider(LOCALHOST, PORT);

        final URI issuer = provider.getIssuer();

        assertNotNull(issuer);
        assertEquals(HTTPS_SCHEME, issuer.getScheme());
        assertEquals(LOCALHOST, issuer.getHost());
        assertEquals(PORT, issuer.getPort());
        assertEquals(EMPTY, issuer.getPath());
        assertNull(issuer.getQuery());
    }

    @Test
    void testGetIssuerNullHostResolved() {
        final String localHost = getLocalHost();
        assumeFalse(localHost == null);

        final StandardIssuerProvider provider = new StandardIssuerProvider(null, PORT);

        final URI issuer = provider.getIssuer();

        assertNotNull(issuer);
        assertEquals(HTTPS_SCHEME, issuer.getScheme());
        assertEquals(localHost, issuer.getHost());
        assertEquals(PORT, issuer.getPort());
        assertEquals(EMPTY, issuer.getPath());
        assertNull(issuer.getQuery());
    }

    private String getLocalHost() {
        try {
            final InetAddress localHostAddress = InetAddress.getLocalHost();
            return localHostAddress.getCanonicalHostName();
        } catch (final UnknownHostException e) {
            return null;
        }
    }
}
