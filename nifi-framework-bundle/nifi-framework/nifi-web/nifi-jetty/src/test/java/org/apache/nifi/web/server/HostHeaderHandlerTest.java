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
package org.apache.nifi.web.server;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HostHeaderHandlerTest {
    private static final String DEFAULT_HOSTNAME = "nifi.apache.org";
    private static final int DEFAULT_PORT = 8080;
    private static final List<String> IPV6_HOSTS = Arrays.asList("ABCD:EF01:2345:6789:ABCD:EF01:2345:6789",
            "2001:DB8:0:0:8:800:200C:417A",
            "FF01:0:0:0:0:0:0:101",
            "0:0:0:0:0:0:0:1",
            "0:0:0:0:0:0:0:0",
            "2001:DB8::8:800:200C:417A",
            "FF01::101",
            "::1",
            "::",
            "0:0:0:0:0:0:13.1.68.3",
            "0:0:0:0:0:FFFF:129.144.52.38",
            "::13.1.68.3",
            "FFFF:129.144.52.38",
            "::FFFF:129.144.52.38");

    private static List<String> defaultHostsAndPorts;

    @BeforeAll
    public static void setUpOnce() throws Exception {
        String actualHostname = InetAddress.getLocalHost().getHostName().toLowerCase();
        List<String> defaultHosts150 = Arrays.asList(DEFAULT_HOSTNAME, "localhost", actualHostname);
        String actualIp = InetAddress.getLocalHost().getHostAddress();
        String loopbackIp = InetAddress.getLoopbackAddress().getHostAddress();
        List<String> defaultHosts = new ArrayList<>(defaultHosts150);
        defaultHosts.remove(DEFAULT_HOSTNAME);
        defaultHosts.addAll(Arrays.asList("[::1]", "127.0.0.1", actualIp, loopbackIp));
        defaultHostsAndPorts = buildHostsWithPorts(defaultHosts, DEFAULT_PORT);
    }

    @Test
    public void testNewConstructorShouldHandleCurrentDefaultValues() {
        HostHeaderHandler handler = new HostHeaderHandler(getNifiProperties(null));

        defaultHostsAndPorts.forEach(host -> assertTrue(handler.hostHeaderIsValid(host)));
    }

    @Test
    public void testShouldParseCustomHostnames() {
        List<String> otherHosts = Arrays.asList("someotherhost.com:9999", "yetanotherbadhost.com", "10.10.10.1:1234", "100.100.100.1");
        NiFiProperties nifiProperties = getNifiProperties(otherHosts);
        HostHeaderHandler handler = new HostHeaderHandler(nifiProperties);
        final List<String> customHostnames = handler.parseCustomHostnames(nifiProperties);

        assertEquals(otherHosts.size() + 2, customHostnames.size()); // Two provided hostnames had ports
        otherHosts.forEach(host -> {
            assertTrue(customHostnames.contains(host));
            String portlessHost = host.split(":", 2)[0];
            assertTrue(customHostnames.contains(portlessHost));
        });
    }

    @Test
    public void testParseCustomHostnamesShouldHandleIPv6WithoutPorts() {
        NiFiProperties nifiProperties = getNifiProperties(IPV6_HOSTS);
        HostHeaderHandler handler = new HostHeaderHandler(nifiProperties);
        List<String> customHostnames = handler.parseCustomHostnames(nifiProperties);

        assertEquals(IPV6_HOSTS.size(), customHostnames.size());
        IPV6_HOSTS.forEach(host -> assertTrue(customHostnames.contains(host)));
    }

    @Test
    public void testParseCustomHostnamesShouldHandleIPv6WithPorts() {
        int port = 1234;
        List<String> ipv6HostsWithPorts = buildHostsWithPorts(IPV6_HOSTS.stream()
                .map(host -> "[" + host + "]")
                .collect(Collectors.toList()), port);
        NiFiProperties nifiProperties = getNifiProperties(ipv6HostsWithPorts);
        HostHeaderHandler handler = new HostHeaderHandler(nifiProperties);
        List<String> customHostnames = handler.parseCustomHostnames(nifiProperties);

        assertEquals(ipv6HostsWithPorts.size() * 2, customHostnames.size());
        ipv6HostsWithPorts.forEach(host -> {
                    assertTrue(customHostnames.contains(host));
                    String portlessHost = StringUtils.substringBeforeLast(host, ":");
                    assertTrue(customHostnames.contains(portlessHost));
                }
        );
    }

    @Test
    public void testShouldIdentifyIPv6Addresses() {
        IPV6_HOSTS.forEach(host -> assertTrue(HostHeaderHandler.isIPv6Address(host)));
    }

    private static List<String> buildHostsWithPorts(List<String> hosts, int port) {
        return hosts.stream()
                .map(host -> host + ":" + port)
                .collect(Collectors.toList());
    }

    private NiFiProperties getNifiProperties(List<String> hosts) {
        Properties bareboneProperties = new Properties();
        bareboneProperties.put(NiFiProperties.WEB_HTTPS_HOST, DEFAULT_HOSTNAME);
        bareboneProperties.put(NiFiProperties.WEB_HTTPS_PORT, Integer.toString(DEFAULT_PORT));

        if(hosts != null) {
            bareboneProperties.put(NiFiProperties.WEB_PROXY_HOST, String.join(",", hosts));
        }

        return new NiFiProperties(bareboneProperties);
    }
}
