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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.util.Set;

public class TlsPlatformTest {
    private static final int ZERO_LENGTH = 0;

    private static final String TLS_1_3 = "TLSv1.3";

    private static final String TLS_1_2 = "TLSv1.2";

    private static final String TLS_1_1 = "TLSv1.1";

    private static final String TLS_1 = "TLSv1";

    private static final String UNEXPECTED_LATEST_PROTOCOL = "Unexpected Latest Protocol [%s]";

    @Test
    public void testGetSupportedProtocolsFound() {
        final Set<String> supportedProtocols = TlsPlatform.getSupportedProtocols();
        assertFalse(supportedProtocols.isEmpty());
    }

    @Test
    public void testGetPreferredProtocolsFound() {
        final Set<String> preferredProtocols = TlsPlatform.getPreferredProtocols();
        assertFalse(preferredProtocols.isEmpty());
    }

    @Test
    public void testGetPreferredProtocolsShouldNotIncludeLegacyProtocols() {
        final Set<String> preferredProtocols = TlsPlatform.getPreferredProtocols();
        assertFalse(preferredProtocols.contains(TLS_1_1));
        assertFalse(preferredProtocols.contains(TLS_1));
    }

    @Test
    public void testGetLatestProtocolFound() {
        final String latestProtocol = TlsPlatform.getLatestProtocol();
        assertNotNull(latestProtocol);
        assertNotEquals(ZERO_LENGTH, latestProtocol.length());
    }

    @Test
    public void testGetLatestProtocolMostRecentVersion() {
        final Set<String> defaultProtocols = TlsPlatform.getSupportedProtocols();
        final String latestProtocol = TlsPlatform.getLatestProtocol();
        if (defaultProtocols.contains(TLS_1_3)) {
            assertEquals(TLS_1_3, latestProtocol);
        } else if (defaultProtocols.contains(TLS_1_2)) {
            assertEquals(TLS_1_2, latestProtocol);
        } else {
            fail(String.format(UNEXPECTED_LATEST_PROTOCOL, latestProtocol));
        }
    }
}
