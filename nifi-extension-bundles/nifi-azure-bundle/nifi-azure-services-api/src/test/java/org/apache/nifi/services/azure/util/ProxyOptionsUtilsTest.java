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
package org.apache.nifi.services.azure.util;

import com.azure.core.http.ProxyOptions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProxyOptionsUtilsTest {

    private static final String DEFAULT_IP_ADDRESS = "192.168.0.1";
    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_USERNAME = "nifi";
    private static final String DEFAULT_PASSWORD = "password";

    private final ProxyOptions proxyOptions1 = createDefaultProxyOptions();

    @Test
    void testEquals() {
        final ProxyOptions proxyOptions2 = createDefaultProxyOptions();

        assertNotEquals(proxyOptions1, proxyOptions2);
        assertTrue(ProxyOptionsUtils.equalsProxyOptions(proxyOptions1, proxyOptions2));
    }

    @Test
    void testEqualsDifferentIpAddress() {
        final ProxyOptions proxyOptions2 = new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress("192.168.0.2", DEFAULT_PORT));

        assertFalse(ProxyOptionsUtils.equalsProxyOptions(proxyOptions1, proxyOptions2));
    }

    @Test
    void testEqualsDifferentPort() {
        final ProxyOptions proxyOptions2 = new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress(DEFAULT_IP_ADDRESS, 8081));

        assertFalse(ProxyOptionsUtils.equalsProxyOptions(proxyOptions1, proxyOptions2));
    }

    @Test
    void testEqualsDifferentUsername() {
        final ProxyOptions proxyOptions2 = createDefaultProxyOptions();
        proxyOptions2.setCredentials("nifi1", DEFAULT_PASSWORD);

        assertFalse(ProxyOptionsUtils.equalsProxyOptions(proxyOptions1, proxyOptions2));
    }

    @Test
    void testEqualsDifferentPassword() {
        final ProxyOptions proxyOptions2 = createDefaultProxyOptions();
        proxyOptions2.setCredentials(DEFAULT_USERNAME, "password1");

        assertFalse(ProxyOptionsUtils.equalsProxyOptions(proxyOptions1, proxyOptions2));
    }

    @Test
    void testEqualsThisNull() {
        assertFalse(ProxyOptionsUtils.equalsProxyOptions(null, proxyOptions1));
    }

    @Test
    void testEqualsThatNull() {
        assertFalse(ProxyOptionsUtils.equalsProxyOptions(proxyOptions1, null));
    }

    @Test
    void testEqualsBothNull() {
        assertTrue(ProxyOptionsUtils.equalsProxyOptions(null, null));
    }

    @Test
    void testHashCode() {
        final ProxyOptions proxyOptions2 = createDefaultProxyOptions();

        assertNotEquals(proxyOptions1.hashCode(), proxyOptions2.hashCode());
        assertEquals(ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions1), ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions2));
    }

    @Test
    void testHashCodeDifferentIpAddress() {
        final ProxyOptions proxyOptions2 = new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress("192.168.0.2", DEFAULT_PORT));

        assertNotEquals(ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions1), ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions2));
    }

    @Test
    void testHashCodeDifferentPort() {
        final ProxyOptions proxyOptions2 = new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress(DEFAULT_IP_ADDRESS, 8081));

        assertNotEquals(ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions1), ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions2));
    }

    @Test
    void testHashCodeDifferentUsername() {
        final ProxyOptions proxyOptions2 = createDefaultProxyOptions();
        proxyOptions2.setCredentials("nifi1", DEFAULT_PASSWORD);

        assertNotEquals(ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions1), ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions2));
    }

    @Test
    void testHashCodeDifferentPassword() {
        final ProxyOptions proxyOptions2 = createDefaultProxyOptions();
        proxyOptions2.setCredentials(DEFAULT_USERNAME, "password1");

        assertNotEquals(ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions1), ProxyOptionsUtils.hashCodeProxyOptions(proxyOptions2));
    }

    @Test
    void testHashCodeNull() {
        assertEquals(0, ProxyOptionsUtils.hashCodeProxyOptions(null));
    }

    private ProxyOptions createDefaultProxyOptions() {
        final ProxyOptions proxyOptions = new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress(DEFAULT_IP_ADDRESS, DEFAULT_PORT));
        proxyOptions.setCredentials(DEFAULT_USERNAME, DEFAULT_PASSWORD);

        return proxyOptions;
    }
}
