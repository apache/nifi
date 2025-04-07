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

import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.configuration.LineEnding;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.StringNettyEventSenderFactory;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestListenSyslog {
    private static final String PRIORITY = "34";
    private static final String TIMESTAMP = "Jan 31 23:59:59";
    private static final String HOST = "localhost.localdomain";
    private static final String BODY = String.class.getName();
    private static final String VALID_MESSAGE = String.format("<%s>%s %s %s", PRIORITY, TIMESTAMP, HOST, BODY);
    private static final String MIME_TYPE = "text/plain";

    private static final boolean STOP_ON_FINISH_DISABLED = false;
    private static final boolean STOP_ON_FINISH_ENABLED = true;
    private static final boolean INITIALIZE_DISABLED = false;
    private static final String LOCALHOST_ADDRESS = "127.0.0.1";
    private static final Duration SENDER_TIMEOUT = Duration.ofSeconds(15);
    private static final Charset CHARSET = StandardCharsets.US_ASCII;

    private TestRunner runner;

    private ListenSyslog processor;

    @BeforeEach
    public void setRunner() {
        processor = new ListenSyslog();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(ListenSyslog.CHARSET, CHARSET.name());
    }

    @AfterEach
    public void closeEventSender() {
        processor.shutdownEventServer();
    }

    @Test
    public void testRunTcp() throws Exception {
        final TransportProtocol protocol = TransportProtocol.TCP;
        runner.setProperty(ListenSyslog.PROTOCOL, protocol.toString());
        runner.setProperty(ListenSyslog.PORT, "0");
        runner.setProperty(ListenSyslog.SOCKET_KEEP_ALIVE, Boolean.FALSE.toString());

        assertSendSuccess(protocol);
    }

    @Test
    public void testRunTcpBatchParseDisabled() throws Exception {
        final TransportProtocol protocol = TransportProtocol.TCP;
        runner.setProperty(ListenSyslog.PROTOCOL, protocol.toString());
        runner.setProperty(ListenSyslog.PORT, "0");
        runner.setProperty(ListenSyslog.SOCKET_KEEP_ALIVE, Boolean.FALSE.toString());
        runner.setProperty(ListenSyslog.PARSE_MESSAGES, Boolean.FALSE.toString());
        runner.setProperty(ListenSyslog.MAX_BATCH_SIZE, "2");

        runner.run(1, STOP_ON_FINISH_DISABLED);
        final int port = ((ListenSyslog) runner.getProcessor()).getListeningPort();

        final String batchedWithEmptyMessages = String.format("%s\n\n%s\n", VALID_MESSAGE, VALID_MESSAGE);
        sendMessages(protocol, port, LineEnding.NONE, batchedWithEmptyMessages);
        runner.run(1, STOP_ON_FINISH_DISABLED, INITIALIZE_DISABLED);

        runner.assertTransferCount(ListenSyslog.REL_INVALID, 0);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS);
        assertEquals(1, successFlowFiles.size(), "Success FlowFiles not matched");

        final MockFlowFile flowFile = successFlowFiles.iterator().next();

        final String batchedMessages = String.format("%s\n%s", VALID_MESSAGE, VALID_MESSAGE);
        flowFile.assertContentEquals(batchedMessages);
    }

    @Test
    public void testRunUdp() throws Exception {
        final TransportProtocol protocol = TransportProtocol.UDP;
        runner.setProperty(ListenSyslog.PROTOCOL, protocol.toString());
        runner.setProperty(ListenSyslog.PORT, "0");

        assertSendSuccess(protocol);
    }

    @Test
    public void testRunUdpBatch() throws Exception {
        final TransportProtocol protocol = TransportProtocol.UDP;
        runner.setProperty(ListenSyslog.PROTOCOL, protocol.toString());
        runner.setProperty(ListenSyslog.PORT, "0");

        final String[] messages = new String[]{VALID_MESSAGE, VALID_MESSAGE};

        runner.setProperty(ListenSyslog.MAX_BATCH_SIZE, Integer.toString(messages.length));
        runner.setProperty(ListenSyslog.PARSE_MESSAGES, Boolean.FALSE.toString());

        runner.run(1, STOP_ON_FINISH_DISABLED);
        final int listeningPort = ((ListenSyslog) runner.getProcessor()).getListeningPort();

        sendMessages(protocol, listeningPort, LineEnding.NONE, messages);
        runner.run(1, STOP_ON_FINISH_ENABLED, INITIALIZE_DISABLED);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS);
        assertEquals(1, successFlowFiles.size(), "Success FlowFiles not matched");

        final Long receivedCounter = runner.getCounterValue(ListenSyslog.RECEIVED_COUNTER);
        assertEquals(Long.valueOf(messages.length), receivedCounter, "Received Counter not matched");
        final Long successCounter = runner.getCounterValue(ListenSyslog.SUCCESS_COUNTER);
        assertEquals(Long.valueOf(1), successCounter, "Success Counter not matched");
    }

    @Test
    public void testRunUdpInvalid() throws Exception {
        final TransportProtocol protocol = TransportProtocol.UDP;
        runner.setProperty(ListenSyslog.PROTOCOL, protocol.toString());
        runner.setProperty(ListenSyslog.PORT, "0");

        runner.run(1, STOP_ON_FINISH_DISABLED);
        final int listeningPort = ((ListenSyslog) runner.getProcessor()).getListeningPort();

        sendMessages(protocol, listeningPort, LineEnding.NONE, TIMESTAMP);
        runner.run(1, STOP_ON_FINISH_ENABLED, INITIALIZE_DISABLED);

        final List<MockFlowFile> invalidFlowFiles = runner.getFlowFilesForRelationship(ListenSyslog.REL_INVALID);
        assertEquals(1, invalidFlowFiles.size(), "Invalid FlowFiles not matched");

        final MockFlowFile flowFile = invalidFlowFiles.iterator().next();
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_SENDER.key(), LOCALHOST_ADDRESS);
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_PROTOCOL.key(), protocol.toString());
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_PORT.key(), Integer.toString(listeningPort));

        final String content = flowFile.getContent();
        assertEquals(TIMESTAMP, content, "FlowFile content not matched");
    }

    private void assertSendSuccess(final TransportProtocol protocol) throws Exception {
        runner.run(1, STOP_ON_FINISH_DISABLED);

        final int port = ((ListenSyslog) runner.getProcessor()).getListeningPort();

        sendMessages(protocol, port, LineEnding.UNIX, VALID_MESSAGE);
        runner.run(1, STOP_ON_FINISH_ENABLED, INITIALIZE_DISABLED);

        final List<MockFlowFile> invalidFlowFiles = runner.getFlowFilesForRelationship(ListenSyslog.REL_INVALID);
        assertTrue(invalidFlowFiles.isEmpty(), "Invalid FlowFiles found");

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS);
        assertEquals(1, successFlowFiles.size(), "Success FlowFiles not matched");

        final MockFlowFile flowFile = successFlowFiles.iterator().next();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), MIME_TYPE);
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_SENDER.key(), LOCALHOST_ADDRESS);
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_PROTOCOL.key(), protocol.toString());
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_PORT.key(), Integer.toString(port));
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_HOSTNAME.key(), HOST);
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_BODY.key(), BODY);
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_PRIORITY.key(), PRIORITY);
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_TIMESTAMP.key(), TIMESTAMP);
        flowFile.assertAttributeEquals(SyslogAttributes.SYSLOG_VALID.key(), Boolean.TRUE.toString());
        flowFile.assertAttributeExists(SyslogAttributes.SYSLOG_FACILITY.key());
        flowFile.assertAttributeExists(SyslogAttributes.SYSLOG_SEVERITY.key());

        final Long receivedCounter = runner.getCounterValue(ListenSyslog.RECEIVED_COUNTER);
        assertEquals(Long.valueOf(1), receivedCounter, "Received Counter not matched");
        final Long successCounter = runner.getCounterValue(ListenSyslog.SUCCESS_COUNTER);
        assertEquals(Long.valueOf(1), successCounter, "Success Counter not matched");

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertFalse(events.isEmpty(), "Provenance Events not found");
        final ProvenanceEventRecord eventRecord = events.iterator().next();
        assertEquals(ProvenanceEventType.RECEIVE, eventRecord.getEventType());
        final String transitUri = String.format("%s://%s:%d", protocol.toString().toLowerCase(), LOCALHOST_ADDRESS, port);
        assertEquals(transitUri, eventRecord.getTransitUri(), "Provenance Transit URI not matched");
    }

    private void sendMessages(final TransportProtocol protocol, final int port, final LineEnding lineEnding, final String... messages) throws Exception {
        final StringNettyEventSenderFactory eventSenderFactory = new StringNettyEventSenderFactory(runner.getLogger(), LOCALHOST_ADDRESS, port, protocol, CHARSET, lineEnding);
        eventSenderFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        eventSenderFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        eventSenderFactory.setTimeout(SENDER_TIMEOUT);
        try (final EventSender<String> eventSender = eventSenderFactory.getEventSender()) {
            for (final String message : messages) {
                eventSender.sendEvent(message);
            }
        }
    }
}
