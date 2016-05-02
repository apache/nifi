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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestPutSplunk {

    private TestRunner runner;
    private TestablePutSplunk proc;
    private CapturingChannelSender sender;

    @Before
    public void init() {
        ComponentLog logger = Mockito.mock(ComponentLog.class);
        sender = new CapturingChannelSender("localhost", 12345, 0, logger);
        proc = new TestablePutSplunk(sender);

        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutSplunk.PORT, "12345");
    }

    @Test
    public void testUDPSendWholeFlowFile() {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.UDP_VALUE.getValue());
        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(1, sender.getMessages().size());
        Assert.assertEquals(message, sender.getMessages().get(0));
    }

    @Test
    public void testTCPSendWholeFlowFile() {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(1, sender.getMessages().size());
        Assert.assertEquals(message + "\n", sender.getMessages().get(0));
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

        Assert.assertEquals(1, sender.getMessages().size());
        Assert.assertEquals(message, sender.getMessages().get(0));
    }

    @Test
    public void testUDPSendDelimitedMessages() {
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.UDP_VALUE.getValue());

        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);

        final String message = "This is message 1DDThis is message 2DDThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(3, sender.getMessages().size());
        Assert.assertEquals("This is message 1", sender.getMessages().get(0));
        Assert.assertEquals("This is message 2", sender.getMessages().get(1));
        Assert.assertEquals("This is message 3", sender.getMessages().get(2));
    }

    @Test
    public void testTCPSendDelimitedMessages() {
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

        Assert.assertEquals(3, sender.getMessages().size());
        Assert.assertEquals("This is message 1\n", sender.getMessages().get(0));
        Assert.assertEquals("This is message 2\n", sender.getMessages().get(1));
        Assert.assertEquals("This is message 3\n", sender.getMessages().get(2));
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

        Assert.assertEquals(3, sender.getMessages().size());
        Assert.assertEquals("This is message 1\n", sender.getMessages().get(0));
        Assert.assertEquals("This is message 2\n", sender.getMessages().get(1));
        Assert.assertEquals("This is message 3\n", sender.getMessages().get(2));
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

        Assert.assertEquals(3, sender.getMessages().size());
        Assert.assertEquals("This is message 1\n", sender.getMessages().get(0));
        Assert.assertEquals("This is message 2\n", sender.getMessages().get(1));
        Assert.assertEquals("This is message 3\n", sender.getMessages().get(2));
    }

    @Test
    public void testTCPSendDelimitedMessagesWithNewLineDelimiter() {
        final String delimiter = "\\n";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        final String message = "This is message 1\nThis is message 2\nThis is message 3";

        runner.enqueue(message);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        mockFlowFile.assertContentEquals(message);

        Assert.assertEquals(3, sender.getMessages().size());
        Assert.assertEquals("This is message 1\n", sender.getMessages().get(0));
        Assert.assertEquals("This is message 2\n", sender.getMessages().get(1));
        Assert.assertEquals("This is message 3\n", sender.getMessages().get(2));
    }

    @Test
    public void testTCPSendDelimitedMessagesWithErrors() {
        sender.setErrorStart(3);
        sender.setErrorEnd(4);

        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        // no delimiter at end
        final String success = "This is message 1DDThis is message 2DD";
        final String failure = "This is message 3DDThis is message 4";
        final String message = success + failure;

        runner.enqueue(message);
        runner.run(1);
        runner.assertTransferCount(PutSplunk.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSplunk.REL_FAILURE, 1);

        // first two messages should went out success
        final MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        successFlowFile.assertContentEquals(success);

        // second two messages should went to failure
        final MockFlowFile failureFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_FAILURE).get(0);
        failureFlowFile.assertContentEquals(failure);

        // should only have the first two messages
        Assert.assertEquals(2, sender.getMessages().size());
        Assert.assertEquals("This is message 1\n", sender.getMessages().get(0));
        Assert.assertEquals("This is message 2\n", sender.getMessages().get(1));
    }

    @Test
    public void testTCPSendDelimitedMessagesWithErrorsInMiddle() {
        sender.setErrorStart(3);
        sender.setErrorEnd(4);

        final String delimiter = "DD";
        runner.setProperty(PutSplunk.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(PutSplunk.PROTOCOL, PutSplunk.TCP_VALUE.getValue());

        // no delimiter at end
        final String success = "This is message 1DDThis is message 2DD";
        final String failure = "This is message 3DDThis is message 4DD";
        final String success2 = "This is message 5DDThis is message 6DDThis is message 7DD";
        final String message = success + failure + success2;

        runner.enqueue(message);
        runner.run(1);
        runner.assertTransferCount(PutSplunk.REL_SUCCESS, 2);
        runner.assertTransferCount(PutSplunk.REL_FAILURE, 1);

        // first two messages should have went out success
        final MockFlowFile successFlowFile1 = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(0);
        successFlowFile1.assertContentEquals(success);

        // last three messages should have went out success
        final MockFlowFile successFlowFile2 = runner.getFlowFilesForRelationship(PutSplunk.REL_SUCCESS).get(1);
        successFlowFile2.assertContentEquals(success2);

        // second two messages should have went to failure
        final MockFlowFile failureFlowFile = runner.getFlowFilesForRelationship(PutSplunk.REL_FAILURE).get(0);
        failureFlowFile.assertContentEquals(failure);

        // should only have the first two messages
        Assert.assertEquals(5, sender.getMessages().size());
        Assert.assertEquals("This is message 1\n", sender.getMessages().get(0));
        Assert.assertEquals("This is message 2\n", sender.getMessages().get(1));
        Assert.assertEquals("This is message 5\n", sender.getMessages().get(2));
        Assert.assertEquals("This is message 6\n", sender.getMessages().get(3));
        Assert.assertEquals("This is message 7\n", sender.getMessages().get(4));
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

        Assert.assertEquals(1, sender.getMessages().size());
        Assert.assertEquals(message, sender.getMessages().get(0));
    }

    @Test
    public void testUnableToCreateConnectionShouldRouteToFailure() {
        PutSplunk proc = new UnableToConnectPutSplunk();
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutSplunk.PORT, "12345");

        final String message = "This is one message, should send the whole FlowFile";

        runner.enqueue(message);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutSplunk.REL_FAILURE, 1);
    }

    /**
     * Extend PutSplunk to use a CapturingChannelSender.
     */
    private static class UnableToConnectPutSplunk extends PutSplunk {

        @Override
        protected ChannelSender createSender(String protocol, String host, int port, int timeout, int maxSendBufferSize, SSLContext sslContext) throws IOException {
            throw new IOException("Unable to create connection");
        }
    }

    /**
     * Extend PutSplunk to use a CapturingChannelSender.
     */
    private static class TestablePutSplunk extends PutSplunk {

        private ChannelSender sender;

        public TestablePutSplunk(ChannelSender channelSender) {
            this.sender = channelSender;
        }

        @Override
        protected ChannelSender createSender(String protocol, String host, int port, int timeout, int maxSendBufferSize, SSLContext sslContext) throws IOException {
            return sender;
        }
    }


    /**
     * A ChannelSender that captures each message that was sent.
     */
    private static class CapturingChannelSender extends ChannelSender {

        private List<String> messages = new ArrayList<>();
        private int count = 0;
        private int errorStart = -1;
        private int errorEnd = -1;

        public CapturingChannelSender(String host, int port, int maxSendBufferSize, ComponentLog logger) {
            super(host, port, maxSendBufferSize, logger);
        }

        @Override
        public void open() throws IOException {

        }

        @Override
        protected void write(byte[] data) throws IOException {
            count++;
            if (errorStart > 0 && count >= errorStart && errorEnd > 0 && count <= errorEnd) {
                throw new IOException("this is an error");
            }
            messages.add(new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public void close() {

        }

        public List<String> getMessages() {
            return messages;
        }

        public void setErrorStart(int errorStart) {
            this.errorStart = errorStart;
        }

        public void setErrorEnd(int errorEnd) {
            this.errorEnd = errorEnd;
        }
    }

}
