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

import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPutSyslog {
    private static final String ADDRESS = "127.0.0.1";

    private static final String LOCALHOST = "localhost";

    private static final String MESSAGE_BODY = String.class.getName();

    private static final String MESSAGE_PRIORITY = "1";

    private static final String DEFAULT_PROTOCOL = "UDP";

    private static final String TIMESTAMP = "Jan 1 00:00:00";

    private static final String VERSION = "2";

    private static final String SYSLOG_MESSAGE = String.format("<%s>%s %s %s", MESSAGE_PRIORITY, TIMESTAMP, LOCALHOST, MESSAGE_BODY);

    private static final String VERSION_SYSLOG_MESSAGE = String.format("<%s>%s %s %s %s", MESSAGE_PRIORITY, VERSION, TIMESTAMP, LOCALHOST, MESSAGE_BODY);

    private static final int MAX_FRAME_LENGTH = 1024;

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final String DELIMITER = "\n";

    private static final int POLL_TIMEOUT_SECONDS = 5;

    private TestRunner runner;

    private final TransportProtocol protocol = TransportProtocol.UDP;

    private int port;
    private EventServer eventServer;
    private BlockingQueue<ByteArrayMessage> messages;

    @BeforeEach
    public void setRunner() throws UnknownHostException {
        final byte[] delimiter = DELIMITER.getBytes(CHARSET);
        messages = new LinkedBlockingQueue<>();

        final InetAddress address = InetAddress.getByName(ADDRESS);
        final NettyEventServerFactory serverFactory = new ByteArrayMessageNettyEventServerFactory(
            new MockComponentLog("id", "TestPutSyslog"), address, 0, protocol, delimiter, MAX_FRAME_LENGTH, messages);
        serverFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        serverFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        this.eventServer = serverFactory.getEventServer();
        this.port = eventServer.getListeningPort();

        runner = TestRunners.newTestRunner(PutSyslog.class);
        runner.setProperty(PutSyslog.HOSTNAME, ADDRESS);
        runner.setProperty(PutSyslog.PROTOCOL, protocol.toString());
        runner.setProperty(PutSyslog.PORT, Integer.toString(port));
        runner.setProperty(PutSyslog.MSG_BODY, MESSAGE_BODY);
        runner.setProperty(PutSyslog.MSG_PRIORITY, MESSAGE_PRIORITY);
        runner.setProperty(PutSyslog.MSG_HOSTNAME, LOCALHOST);
        runner.setProperty(PutSyslog.MSG_TIMESTAMP, TIMESTAMP);
        runner.assertValid();
    }

    @AfterEach
    public void shutdownEventServer() {
        if (eventServer != null) {
            eventServer.shutdown();
        }
    }

    @Test
    public void testRunNoFlowFiles() {
        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testRunSuccess() throws InterruptedException {
        assertSyslogMessageSuccess(SYSLOG_MESSAGE, Collections.emptyMap());
    }

    @Test
    public void testRunSuccessSyslogVersion() throws InterruptedException {
        final String versionAttributeKey = "version";
        runner.setProperty(PutSyslog.MSG_VERSION, String.format("${%s}", versionAttributeKey));
        final Map<String, String> attributes = Collections.singletonMap(versionAttributeKey, VERSION);

        assertSyslogMessageSuccess(VERSION_SYSLOG_MESSAGE, attributes);
    }

    @Test
    public void testRunInvalid() {
        runner.setProperty(PutSyslog.MSG_PRIORITY, Integer.toString(Integer.MAX_VALUE));
        runner.enqueue(new byte[]{});
        runner.run();
        runner.assertAllFlowFilesTransferred(PutSyslog.REL_INVALID);
    }

    @Test
    public void testRunFailure() {
        runner.setProperty(PutSyslog.PROTOCOL, PutSyslog.TCP_VALUE);
        runner.setProperty(PutSyslog.HOSTNAME, "this-host-does-not-exist");
        runner.setProperty(PutSyslog.PORT, "44");
        runner.enqueue(new byte[]{});
        runner.run();
        runner.assertAllFlowFilesTransferred(PutSyslog.REL_FAILURE);
    }

    private void assertSyslogMessageSuccess(final String expectedSyslogMessage, final Map<String, String> attributes) throws InterruptedException {
        try {
            runner.enqueue(expectedSyslogMessage, attributes);
            runner.run();

            final ByteArrayMessage message = messages.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertNotNull(message);
            final String syslogMessage = new String(message.getMessage(), CHARSET);
            runner.assertAllFlowFilesTransferred(PutSyslog.REL_SUCCESS);

            assertEquals(expectedSyslogMessage, syslogMessage);
            assertProvenanceRecordTransitUriFound();
        } finally {
            eventServer.shutdown();
        }
    }

    private void assertProvenanceRecordTransitUriFound() {
        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertFalse(provenanceEvents.isEmpty(), "Provenance Events not found");
        final ProvenanceEventRecord provenanceEventRecord = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.SEND, provenanceEventRecord.getEventType());

        final String transitUri = provenanceEventRecord.getTransitUri();
        assertNotNull(transitUri, "Transit URI not found");
        assertTrue(transitUri.contains(DEFAULT_PROTOCOL), "Transit URI Protocol not found");
        assertTrue(transitUri.contains(ADDRESS), "Transit URI Hostname not found");
        assertTrue(transitUri.contains(Integer.toString(port)), "Transit URI Port not found");
    }
}
