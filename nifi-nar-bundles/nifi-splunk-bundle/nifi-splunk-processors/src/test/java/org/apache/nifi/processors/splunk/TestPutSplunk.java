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
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TestPutSplunk {

    private TestRunner runner;
    private BlockingQueue<ByteArrayMessage> messages;
    private EventServer eventServer;
    private final static String OUTGOING_MESSAGE_DELIMITER = "\n";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final static int VALID_LARGE_FILE_SIZE = 32768;
    private final int PORT = 12345;

    @Before
    public void setup() throws Exception {
        ComponentLog logger = Mockito.mock(ComponentLog.class);
        runner = TestRunners.newTestRunner(PutSplunk.class);
        runner.setProperty(PutSplunk.PORT, String.valueOf(PORT));
        createTestServer("localhost", PORT, TransportProtocol.TCP, null);
    }

    @After
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
    public void testUDPSendWholeFlowFile() throws InterruptedException {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.UDP_VALUE.getValue());
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, OUTGOING_MESSAGE_DELIMITER);
        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        TimeUnit.MILLISECONDS.sleep(300);
        Assert.assertEquals(1, messages.size());
        ByteArrayMessage receivedMessage = messages.poll();
        Assert.assertEquals(message, new String(receivedMessage.getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testTCPSendWholeFlowFile() throws InterruptedException {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        TimeUnit.MILLISECONDS.sleep(300);
        Assert.assertEquals(1, messages.size());
        Assert.assertEquals(message, new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testTCPSendMultipleFlowFiles() {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.enqueue(message);
        runner.run(2);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 2);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(2, messages.size());
        Assert.assertEquals(message, new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals(message, new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testTCPSendWholeFlowFileAlreadyHasNewLine() {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        final String message = "This is one message, should send the whole FlowFile\n";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(1, messages.size());
        Assert.assertEquals(message, new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testUDPSendDelimitedMessages() throws Exception {
        shutdownServer();
        createTestServer("localhost", PORT, TransportProtocol.UDP, null);
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.UDP_VALUE.getValue());

        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);

        final String message = "This is message 1DDThis is message 2DDThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(3, messages.size());
        Assert.assertEquals("This is message 1", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 2", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 3", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testTCPSendDelimitedMessages() throws InterruptedException {
        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        // no delimiter at end
        final String message = "This is message 1DDThis is message 2DDThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        TimeUnit.MILLISECONDS.sleep(300);
        Assert.assertEquals(3, messages.size());
        Assert.assertEquals("This is message 1", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 2", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 3", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testTCPSendDelimitedMessagesWithEL() {
        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, "${flow.file.delim}");
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        // no delimiter at end
        final String message = "This is message 1DDThis is message 2DDThis is message 3";

        final Map<String,String> attrs = new HashMap<>();
        attrs.put("flow.file.delim", delimiter);

        runner.enqueue(message, attrs);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(3, messages.size());
        Assert.assertEquals("This is message 1", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 2", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 3", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testTCPSendDelimitedMessagesEndsWithDelimiter() {
        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        // delimiter at end
        final String message = "This is message 1DDThis is message 2DDThis is message 3DD";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(3, messages.size());
        Assert.assertEquals("This is message 1", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 2", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 3", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testTCPSendDelimitedMessagesWithNewLineDelimiter() {
        final String delimiter = "\\n";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());
        runner.setProperty(PutSplunk.CHARSET, "UTF-8");

        final String message = "This is message 1\nThis is message 2\nThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(3, messages.size());
        Assert.assertEquals("This is message 1", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 2", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
        Assert.assertEquals("This is message 3", new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testCompletingPreviousBatchOnNextExecution() {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.UDP_VALUE.getValue());

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run(2, false); // don't shutdown to prove that next onTrigger complete previous batch
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(1, messages.size());
        Assert.assertEquals(message, new String(messages.poll().getMessage(), StandardCharsets.UTF_8));
    }

    @Test
    public void testUnableToCreateConnectionShouldRouteToFailure() throws InterruptedException {
        runner.setProperty(PutSplunk.PORT, "11111");

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run();
        TimeUnit.MILLISECONDS.sleep(300);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_FAILURE, 1);
    }

    private void createTestServer(final String address, final int port, final TransportProtocol protocol, final SSLContext sslContext) throws Exception {
        messages = new LinkedBlockingQueue<>();
        final byte[] delimiter = OUTGOING_MESSAGE_DELIMITER.getBytes(CHARSET);
        NettyEventServerFactory serverFactory = new ByteArrayMessageNettyEventServerFactory(runner.getLogger(), address, port, protocol, delimiter, VALID_LARGE_FILE_SIZE, messages);
        if (sslContext != null) {
            serverFactory.setSslContext(sslContext);
        }
        eventServer = serverFactory.getEventServer();
    }
}
