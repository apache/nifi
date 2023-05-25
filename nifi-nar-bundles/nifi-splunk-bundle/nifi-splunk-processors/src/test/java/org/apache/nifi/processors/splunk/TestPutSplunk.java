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
package org.apache.nifi.processors.splunk;

import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


public class TestPutSplunk {

    private TestRunner runner;
    private BlockingQueue<ByteArrayMessage> messages;
    private EventServer eventServer;
    private final static int DEFAULT_TEST_TIMEOUT_PERIOD = 10000;
    private final static String OUTGOING_MESSAGE_DELIMITER = "\n";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final static int VALID_LARGE_FILE_SIZE = 32768;
    private static final String LOCALHOST = "localhost";

    @BeforeEach
    public void setup() {
        runner = TestRunners.newTestRunner(PutSplunk.class);
    }

    @AfterEach
    public void cleanup() {
        runner.shutdown();
        shutdownServer();
    }

    private void shutdownServer() {
        if (eventServer != null) {
            eventServer.shutdown();
        }
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testUDPSendWholeFlowFile() throws Exception {
        createTestServer(TransportProtocol.UDP);
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, OUTGOING_MESSAGE_DELIMITER);
        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData(message);
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testTCPSendWholeFlowFile() throws Exception {
        createTestServer(TransportProtocol.TCP);
        final String message = "This is one message, should send the whole FlowFile";
        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData(message);
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testTCPSendMultipleFlowFiles() throws Exception {
        createTestServer(TransportProtocol.TCP);

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.enqueue(message);
        runner.run(2);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 2);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData(message, message);
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testTCPSendWholeFlowFileAlreadyHasNewLine() throws Exception {
        createTestServer(TransportProtocol.TCP);

        final String message = "This is one message, should send the whole FlowFile\n";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData(message.trim());
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testUDPSendDelimitedMessages() throws Exception {
        createTestServer(TransportProtocol.UDP);
        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);

        final String message = "This is message 1DDThis is message 2DDThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData("This is message 1", "This is message 2", "This is message 3");
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testTCPSendDelimitedMessages() throws Exception {
        createTestServer(TransportProtocol.TCP);
        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);

        // no delimiter at end
        final String message = "This is message 1DDThis is message 2DDThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData("This is message 1", "This is message 2", "This is message 3");
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testTCPSendDelimitedMessagesWithEL() throws Exception {
        createTestServer(TransportProtocol.TCP);

        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, "${flow.file.delim}");

        // no delimiter at end
        final String message = "This is message 1DDThis is message 2DDThis is message 3";

        final Map<String,String> attrs = new HashMap<>();
        attrs.put("flow.file.delim", delimiter);

        runner.enqueue(message, attrs);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData("This is message 1", "This is message 2", "This is message 3");
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testTCPSendDelimitedMessagesEndsWithDelimiter() throws Exception {
        createTestServer(TransportProtocol.TCP);
        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);

        // delimiter at end
        final String message = "This is message 1DDThis is message 2DDThis is message 3DD";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData("This is message 1", "This is message 2", "This is message 3");

    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testTCPSendDelimitedMessagesWithNewLineDelimiter() throws Exception {
        createTestServer(TransportProtocol.TCP);
        final String delimiter = "\\n";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(PutSplunk.CHARSET, "UTF-8");

        final String message = "This is message 1\nThis is message 2\nThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData("This is message 1", "This is message 2", "This is message 3");
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testCompletingPreviousBatchOnNextExecution() throws Exception {
        createTestServer(TransportProtocol.UDP);

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run(2, false); // don't shutdown to prove that next onTrigger complete previous batch
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        checkReceivedAllData(message);
    }

    @Test
    @Timeout(value = DEFAULT_TEST_TIMEOUT_PERIOD, unit = TimeUnit.MILLISECONDS)
    public void testUnableToCreateConnectionShouldRouteToFailure() {
        // Set an unreachable port
        runner.setProperty(PutSplunk.PORT, "482");
        runner.setProperty(PutSplunk.HOSTNAME, "host-that-does-not-exist");

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_FAILURE, 1);
    }


    private void createTestServer(final TransportProtocol protocol) {
        messages = new LinkedBlockingQueue<>();
        runner.setProperty(PutSplunk.PROTOCOL, protocol.name());
        final byte[] delimiter = OUTGOING_MESSAGE_DELIMITER.getBytes(CHARSET);

        NettyEventServerFactory serverFactory = new ByteArrayMessageNettyEventServerFactory(runner.getLogger(), getListenAddress(), 0, protocol, delimiter, VALID_LARGE_FILE_SIZE, messages);
        serverFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        serverFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        eventServer = serverFactory.getEventServer();
        runner.setProperty(PutSplunk.PORT, String.valueOf(eventServer.getListeningPort()));
    }

    private void checkReceivedAllData(final String... sentData) throws Exception {
        // check each sent FlowFile was successfully sent and received.
        for (String item : sentData) {
            ByteArrayMessage packet = messages.take();
            assertNotNull(packet);
            assertArrayEquals(item.getBytes(), packet.getMessage());
        }

        assertNull(messages.poll(), "Unexpected extra messages found");
    }

    private InetAddress getListenAddress() {
        try {
            return InetAddress.getByName(LOCALHOST);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
