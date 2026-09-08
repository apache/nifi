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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
public class TestPutUDPPayloadLimit {

    private static final String IPV4_LOOPBACK = "127.0.0.1";
    private static final String IPV6_LOOPBACK = "::1";
    private static final String IPV4_MAPPED_LOOPBACK = "::ffff:127.0.0.1";
    private static final String UNKNOWN_HOST = "nifi-putudp-unknown-host.invalid";
    private static final int UNUSED_PORT = 49_152;
    private static final int IPV4_OVERSIZE = PutUDP.MAX_IPV4_UDP_PAYLOAD_LENGTH + 1;
    private static final int IPV6_OVERSIZE = PutUDP.MAX_IPV6_UDP_PAYLOAD_LENGTH + 1;

    private TestRunner runner;

    @BeforeEach
    public void setup() {
        runner = TestRunners.newTestRunner(PutUDP.class);
        runner.setProperty(PutUDP.PORT, Integer.toString(UNUSED_PORT));
    }

    @AfterEach
    public void cleanup() {
        runner.shutdown();
    }

    @Test
    public void testMaxPayloadLengthIPv4Loopback() {
        assertEquals(PutUDP.MAX_IPV4_UDP_PAYLOAD_LENGTH, PutUDP.maxPayloadLength(IPV4_LOOPBACK));
    }

    @Test
    @EnabledIf("ipv6LoopbackAvailable")
    public void testMaxPayloadLengthIPv6Loopback() {
        assertEquals(PutUDP.MAX_IPV6_UDP_PAYLOAD_LENGTH, PutUDP.maxPayloadLength(IPV6_LOOPBACK));
    }

    @Test
    public void testMaxPayloadLengthIPv4MappedIPv6UsesIPv4Ceiling() throws UnknownHostException {
        final InetAddress mapped = InetAddress.getByName(IPV4_MAPPED_LOOPBACK);
        assertFalse(PutUDP.isUnmappedIPv6Address(mapped));
        assertEquals(PutUDP.MAX_IPV4_UDP_PAYLOAD_LENGTH, PutUDP.maxPayloadLength(IPV4_MAPPED_LOOPBACK));
    }

    @Test
    public void testMaxPayloadLengthUnknownHostUsesIPv6Ceiling() {
        assertEquals(PutUDP.MAX_IPV6_UDP_PAYLOAD_LENGTH, PutUDP.maxPayloadLength(UNKNOWN_HOST));
    }

    @Test
    public void testIPv4DestinationRejectsPayloadAboveIPv4Maximum() {
        configureHostname(IPV4_LOOPBACK);
        runner.enqueue(new byte[IPV4_OVERSIZE]);
        runner.run();

        assertSizeRejected("IPv4", PutUDP.MAX_IPV4_UDP_PAYLOAD_LENGTH);
    }

    @Test
    public void testIPv4DestinationRejectsPayloadAboveIPv6Maximum() {
        configureHostname(IPV4_LOOPBACK);
        runner.enqueue(new byte[IPV6_OVERSIZE]);
        runner.run();

        assertSizeRejected("IPv4", PutUDP.MAX_IPV4_UDP_PAYLOAD_LENGTH);
    }

    @Test
    @EnabledIf("ipv6LoopbackAvailable")
    public void testIPv6DestinationAcceptsPayloadBetweenIPv4AndIPv6Maxima() {
        configureHostname(IPV6_LOOPBACK);
        runner.enqueue(new byte[IPV4_OVERSIZE]);
        runner.run();

        assertFalse(loggedSizeRejection(), "IPv6 destination must not apply the IPv4 payload ceiling");
        runner.assertQueueEmpty();
        assertEquals(1, runner.getFlowFilesForRelationship(PutUDP.REL_SUCCESS).size()
                + runner.getFlowFilesForRelationship(PutUDP.REL_FAILURE).size());
    }

    @Test
    @EnabledIf("ipv6LoopbackAvailable")
    public void testIPv6DestinationRejectsPayloadAboveIPv6Maximum() {
        configureHostname(IPV6_LOOPBACK);
        runner.enqueue(new byte[IPV6_OVERSIZE]);
        runner.run();

        assertSizeRejected("IPv6", PutUDP.MAX_IPV6_UDP_PAYLOAD_LENGTH);
    }

    @Test
    public void testUnknownHostDoesNotApplyIPv4Ceiling() {
        configureHostname(UNKNOWN_HOST);
        runner.enqueue(new byte[IPV4_OVERSIZE]);
        runner.run();

        assertFalse(loggedSizeRejection(), "Unknown host must not apply the IPv4 payload ceiling");
        runner.assertTransferCount(PutUDP.REL_FAILURE, 1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testUnknownHostRejectsPayloadAboveIPv6Maximum() {
        configureHostname(UNKNOWN_HOST);
        runner.enqueue(new byte[IPV6_OVERSIZE]);
        runner.run();

        assertSizeRejected("IPv6", PutUDP.MAX_IPV6_UDP_PAYLOAD_LENGTH);
    }

    static boolean ipv6LoopbackAvailable() {
        try {
            return PutUDP.isUnmappedIPv6Address(InetAddress.getByName(IPV6_LOOPBACK));
        } catch (final UnknownHostException e) {
            return false;
        }
    }

    private void configureHostname(final String hostname) {
        runner.setProperty(PutUDP.HOSTNAME, hostname);
        runner.assertValid();
    }

    private void assertSizeRejected(final String addressFamily, final int maxPayloadLength) {
        runner.assertTransferCount(PutUDP.REL_SUCCESS, 0);
        runner.assertTransferCount(PutUDP.REL_FAILURE, 1);
        runner.assertQueueEmpty();
        assertTrue(runner.getLogger().getErrorMessages().stream()
                .anyMatch(message -> message.getMsg().contains("exceeds the " + addressFamily + " maximum payload of " + maxPayloadLength)));
        assertFalse(runner.getLogger().getErrorMessages().stream()
                .anyMatch(message -> message.getMsg().contains("Send Failed")));
    }

    private boolean loggedSizeRejection() {
        return runner.getLogger().getErrorMessages().stream()
                .anyMatch(message -> message.getMsg().contains("exceeds the"));
    }
}
