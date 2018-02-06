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
package org.apache.nifi.remote;

import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPeerDescriptionModifier {

    @Test
    public void testNoConfiguration() {
        Properties props = new Properties();
        final NiFiProperties properties = new StandardNiFiProperties(props);
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier(properties);
        assertFalse(modifier.isModificationNeeded(SiteToSiteTransportProtocol.RAW));
        assertFalse(modifier.isModificationNeeded(SiteToSiteTransportProtocol.HTTP));
    }

    @Test
    public void testInvalidNoHostname() {
        Properties props = new Properties();
        props.put("nifi.remote.route.raw.no-host.when", "true");
        final NiFiProperties properties = new StandardNiFiProperties(props);
        try {
            new PeerDescriptionModifier(properties);
            fail("Should throw an Exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Found an invalid Site-to-Site route definition [no-host] 'hostname' is not specified.", e.getMessage());
        }
    }

    @Test
    public void testInvalidNoPort() {
        Properties props = new Properties();
        props.put("nifi.remote.route.raw.no-port.when", "true");
        props.put("nifi.remote.route.raw.no-port.hostname", "proxy.example.com");
        final NiFiProperties properties = new StandardNiFiProperties(props);
        try {
            new PeerDescriptionModifier(properties);
            fail("Should throw an Exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Found an invalid Site-to-Site route definition [no-port] 'port' is not specified.", e.getMessage());
        }
    }

    @Test
    public void testInvalidConfigurationName() {
        Properties props = new Properties();
        props.put("nifi.remote.route.raw.invalid-name.when", "true");
        props.put("nifi.remote.route.raw.invalid-name.hostname", "proxy.example.com");
        props.put("nifi.remote.route.raw.invalid-name.port", "8081");
        props.put("nifi.remote.route.raw.invalid-name.secure", "true");
        props.put("nifi.remote.route.raw.invalid-name.unsupported", "true");
        final NiFiProperties properties = new StandardNiFiProperties(props);
        try {
            new PeerDescriptionModifier(properties);
            fail("Should throw an Exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Found an invalid Site-to-Site route definition property 'nifi.remote.route.raw.invalid-name.unsupported'." +
                    " Routing property keys should be formatted as 'nifi.remote.route.{protocol}.{name}.{routingConfigName}'." +
                    " Where {protocol} is 'raw' or 'http', and {routingConfigName} is 'when', 'hostname', 'port' or 'secure'.", e.getMessage());
        }
    }

    @Test
    public void testInvalidPropertyKeyNoProtocol() {
        Properties props = new Properties();
        props.put("nifi.remote.route.", "true");
        final NiFiProperties properties = new StandardNiFiProperties(props);
        try {
            new PeerDescriptionModifier(properties);
            fail("Should throw an Exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Found an invalid Site-to-Site route definition property 'nifi.remote.route.'." +
                    " Routing property keys should be formatted as 'nifi.remote.route.{protocol}.{name}.{routingConfigName}'." +
                    " Where {protocol} is 'raw' or 'http', and {routingConfigName} is 'when', 'hostname', 'port' or 'secure'.", e.getMessage());
        }
    }

    @Test
    public void testInvalidPropertyKeyNoName() {
        Properties props = new Properties();
        props.put("nifi.remote.route.http.", "true");
        final NiFiProperties properties = new StandardNiFiProperties(props);
        try {
            new PeerDescriptionModifier(properties);
            fail("Should throw an Exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Found an invalid Site-to-Site route definition property 'nifi.remote.route.http.'." +
                    " Routing property keys should be formatted as 'nifi.remote.route.{protocol}.{name}.{routingConfigName}'." +
                    " Where {protocol} is 'raw' or 'http', and {routingConfigName} is 'when', 'hostname', 'port' or 'secure'.", e.getMessage());
        }
    }

    @Test
    public void testInvalidExpression() {
        Properties props = new Properties();
        props.put("nifi.remote.route.raw.invalid-el.when", "${nonExistingFunction()}");
        props.put("nifi.remote.route.raw.invalid-el.hostname", "proxy.example.com");
        props.put("nifi.remote.route.raw.invalid-el.port", "8081");
        final NiFiProperties properties = new StandardNiFiProperties(props);
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier(properties);

        final PeerDescription source = new PeerDescription("client", 12345, true);
        final PeerDescription target = new PeerDescription("nifi0", 8081, true);

        try {
            modifier.modify(source, target,
                    SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.Peers, new HashMap<>());
            fail("Should throw an Exception");
        } catch (AttributeExpressionLanguageException e) {
            assertTrue(e.getMessage().startsWith("Invalid Expression"));
        }
    }

    @Test
    public void testDefaultIsNotSecure() {
        Properties props = new Properties();
        props.put("nifi.remote.route.raw.no-port.when", "true");
        props.put("nifi.remote.route.raw.no-port.hostname", "proxy.example.com");
        props.put("nifi.remote.route.raw.no-port.port", "8443");
        final NiFiProperties properties = new StandardNiFiProperties(props);
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier(properties);

        final PeerDescription source = new PeerDescription("client", 12345, true);
        final PeerDescription target = new PeerDescription("nifi0", 8081, true);
        final PeerDescription modifiedTarget = modifier.modify(source, target,
                SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.Peers, new HashMap<>());
        assertFalse(modifiedTarget.isSecure());
    }

    @Test
    public void testRawPortToNode() {
        Properties props = new Properties();

        // RAW S2S route configs.
        // Port number to Node
        // proxy1.example.com:17491 -> nifi0:8081
        // proxy1.example.com:17492 -> nifi1:8081
        props.put("nifi.remote.route.raw.port-to-node.when", "${X-ProxyHost:equals('proxy1.example.com')" +
                ":or(${s2s.source.hostname:equals('proxy1.example.com')})}");
        props.put("nifi.remote.route.raw.port-to-node.hostname", "proxy1.example.com");
        props.put("nifi.remote.route.raw.port-to-node.port",
                "${s2s.target.hostname:equals('nifi0'):ifElse('17491'," +
                        "${s2s.target.hostname:equals('nifi1'):ifElse('17492', 'undefined')})}");
        props.put("nifi.remote.route.raw.port-to-node.secure", "true");

        // Other S2S configs.
        props.put("nifi.remote.input.host", "node0");
        props.put("nifi.remote.input.secure", "true");
        props.put("nifi.remote.input.socket.port", "8081");
        props.put("nifi.remote.input.http.enabled", "true");

        final NiFiProperties properties = new StandardNiFiProperties(props);
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier(properties);

        // For requests coming from the proxy server, modify target description,
        // so that client can send further request to the proxy.
        // To nifi0.
        PeerDescription source = new PeerDescription("proxy1.example.com", 12345, true);
        PeerDescription target = new PeerDescription("nifi0", 8081, true);
        PeerDescription modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());

        assertNotNull(modifiedTarget);
        assertEquals("proxy1.example.com", modifiedTarget.getHostname());
        assertEquals(17491, modifiedTarget.getPort());
        assertEquals(true, modifiedTarget.isSecure());

        // To nifi1.
        target = new PeerDescription("nifi1", 8081, true);
        modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());

        assertNotNull(modifiedTarget);
        assertEquals("proxy1.example.com", modifiedTarget.getHostname());
        assertEquals(17492, modifiedTarget.getPort());
        assertEquals(true, modifiedTarget.isSecure());

        // For requests coming directly, use the original target description.
        source = new PeerDescription("192.168.1.101", 23456, true);
        modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());
        assertNotNull(modifiedTarget);
        assertEquals(target, modifiedTarget);

    }

    @Test
    public void testRawServerNameToNode() {
        Properties props = new Properties();

        // RAW S2S route configs.
        // Server name to Node
        // nifi0.example.com:17491 -> nifi0:8081
        // nifi1.example.com:17491 -> nifi1:8081
        props.put("nifi.remote.route.raw.name-to-node.when", "${X-ProxyHost:contains('.example.com')" +
                ":or(${s2s.source.hostname:contains('.example.com')})}");
        props.put("nifi.remote.route.raw.name-to-node.hostname", "${s2s.target.hostname}.example.com");
        props.put("nifi.remote.route.raw.name-to-node.port", "17491");
        props.put("nifi.remote.route.raw.name-to-node.secure", "true");

        // Other S2S configs.
        props.put("nifi.remote.input.host", "node0");
        props.put("nifi.remote.input.secure", "true");
        props.put("nifi.remote.input.socket.port", "8081");
        props.put("nifi.remote.input.http.enabled", "true");

        final NiFiProperties properties = new StandardNiFiProperties(props);
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier(properties);

        // For requests coming from the proxy server, modify target description,
        // so that client can send further request to the proxy.
        // To nifi0.
        PeerDescription source = new PeerDescription("nifi0.example.com", 12345, true);
        PeerDescription target = new PeerDescription("nifi0", 8081, true);
        PeerDescription modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());

        assertNotNull(modifiedTarget);
        assertEquals("nifi0.example.com", modifiedTarget.getHostname());
        assertEquals(17491, modifiedTarget.getPort());
        assertEquals(true, modifiedTarget.isSecure());

        // To nifi1.
        target = new PeerDescription("nifi1", 8081, true);
        modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());

        assertNotNull(modifiedTarget);
        assertEquals("nifi1.example.com", modifiedTarget.getHostname());
        assertEquals(17491, modifiedTarget.getPort());
        assertEquals(true, modifiedTarget.isSecure());

        // For requests coming directly, use the original target description.
        source = new PeerDescription("192.168.1.101", 23456, true);
        modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());
        assertNotNull(modifiedTarget);
        assertEquals(target, modifiedTarget);

    }

    @Test
    public void testHttpsTerminate() {
        Properties props = new Properties();

        // https://nifi0.example.com -> http://nifi0:8080
        // https://nifi1.example.com -> http://nifi1:8080
        // S2S HTTP configs.
        props.put("nifi.remote.route.http.terminate.when", "${X-ProxyHost:contains('.example.com')" +
                ":or(${s2s.source.hostname:contains('.example.com')})}");
        props.put("nifi.remote.route.http.terminate.hostname", "${s2s.target.hostname}.example.com");
        props.put("nifi.remote.route.http.terminate.port", "443");
        props.put("nifi.remote.route.http.terminate.secure", "true");

        // Other S2S configs.
        props.put("nifi.web.http.host", "nifi0");
        props.put("nifi.web.http.port", "8080");
        props.put("nifi.remote.input.host", "nifi0");
        props.put("nifi.remote.input.secure", "false");
        props.put("nifi.remote.input.socket.port", "");
        props.put("nifi.remote.input.http.enabled", "true");


        final NiFiProperties properties = new StandardNiFiProperties(props);
        final PeerDescriptionModifier modifier = new PeerDescriptionModifier(properties);

        // For requests coming from the proxy server, modify target description,
        // so that client can send further request to the proxy.
        // To nifi0.
        PeerDescription source = new PeerDescription("nifi0.example.com", 12345, true);
        PeerDescription target = new PeerDescription("nifi0", 8080, false);
        final Map<String, String> proxyHeders = new HashMap<>();
        proxyHeders.put("X-ProxyHost", "nifi0.example.com:443");
        proxyHeders.put("X-Forwarded-For", "172.16.1.103");
        PeerDescription modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.HTTP, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>(proxyHeders));

        assertNotNull(modifiedTarget);
        assertEquals("nifi0.example.com", modifiedTarget.getHostname());
        assertEquals(443, modifiedTarget.getPort());
        assertEquals(true, modifiedTarget.isSecure());

        // To nifi1.
        proxyHeders.put("X-ProxyHost", "nifi1.example.com:443");
        target = new PeerDescription("nifi1", 8081, true);
        modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.HTTP, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>(proxyHeders));

        assertNotNull(modifiedTarget);
        assertEquals("nifi1.example.com", modifiedTarget.getHostname());
        assertEquals(443, modifiedTarget.getPort());
        assertEquals(true, modifiedTarget.isSecure());

        // For requests coming directly, use the original target description.
        source = new PeerDescription("192.168.1.101", 23456, true);
        modifiedTarget = modifier.modify(source, target, SiteToSiteTransportProtocol.HTTP, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>());
        assertNotNull(modifiedTarget);
        assertEquals(target, modifiedTarget);
    }
}
