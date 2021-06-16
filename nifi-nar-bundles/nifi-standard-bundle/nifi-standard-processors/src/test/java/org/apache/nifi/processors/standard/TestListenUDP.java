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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TestListenUDP {

    private static final String LOCALHOST = "localhost";

    private int port = 0;

    private TestRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ListenUDP.class);
        port = NetworkUtils.availablePort();
        runner.setProperty(ListenUDP.PORT, Integer.toString(port));
    }

    @Test
    public void testCustomValidation() {
        runner.setProperty(ListenUDP.PORT, "1");
        runner.assertValid();

        runner.setProperty(ListenUDP.SENDING_HOST, "localhost");
        runner.assertNotValid();

        runner.setProperty(ListenUDP.SENDING_HOST_PORT, "1234");
        runner.assertValid();

        runner.setProperty(ListenUDP.SENDING_HOST, "");
        runner.assertNotValid();
    }

    @Test
    public void testDefaultBehavior() throws IOException, InterruptedException {
        final List<String> messages = getMessages(15);
        final int expectedQueued = messages.size();
        final int expectedTransferred = messages.size();

        // default behavior should produce a FlowFile per message sent

        run(new DatagramSocket(), messages, expectedQueued, expectedTransferred);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, messages.size());

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenUDP.REL_SUCCESS);
        verifyFlowFiles(mockFlowFiles);
        verifyProvenance(expectedTransferred);
    }

    @Test
    public void testSendingMoreThanQueueSize() throws IOException, InterruptedException {
        final int maxQueueSize = 3;
        runner.setProperty(ListenUDP.MAX_MESSAGE_QUEUE_SIZE, String.valueOf(maxQueueSize));

        final List<String> messages = getMessages(20);

        run(new DatagramSocket(), messages, maxQueueSize, maxQueueSize);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, maxQueueSize);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenUDP.REL_SUCCESS);
        verifyFlowFiles(mockFlowFiles);
        verifyProvenance(maxQueueSize);
    }

    @Test
    public void testBatchingSingleSender() throws IOException, InterruptedException {
        final String delimiter = "NN";
        runner.setProperty(ListenUDP.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(ListenUDP.MAX_BATCH_SIZE, "3");

        final List<String> messages = getMessages(5);
        final int expectedQueued = messages.size();
        final int expectedTransferred = 2;

        run(new DatagramSocket(), messages, expectedQueued, expectedTransferred);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, expectedTransferred);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenUDP.REL_SUCCESS);

        MockFlowFile mockFlowFile1 = mockFlowFiles.get(0);
        mockFlowFile1.assertContentEquals("This is message 1" + delimiter + "This is message 2" + delimiter + "This is message 3");

        MockFlowFile mockFlowFile2 = mockFlowFiles.get(1);
        mockFlowFile2.assertContentEquals("This is message 4" + delimiter + "This is message 5");

        verifyProvenance(expectedTransferred);
    }

    @Test
    public void testBatchingWithDifferentSenders() {
        final String sender1 = "sender1";
        final String sender2 = "sender2";
        final ChannelResponder responder = Mockito.mock(ChannelResponder.class);
        final byte[] message = "test message".getBytes(StandardCharsets.UTF_8);

        final List<StandardEvent> mockEvents = new ArrayList<>();
        mockEvents.add(new StandardEvent(sender1, message, responder));
        mockEvents.add(new StandardEvent(sender1, message, responder));
        mockEvents.add(new StandardEvent(sender2, message, responder));
        mockEvents.add(new StandardEvent(sender2, message, responder));

        MockListenUDP mockListenUDP = new MockListenUDP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenUDP);
        runner.setProperty(ListenRELP.PORT, "1");
        runner.setProperty(ListenRELP.MAX_BATCH_SIZE, "10");

        // sending 4 messages with a batch size of 10, but should get 2 FlowFiles because of different senders
        runner.run();
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 2);

        verifyProvenance(2);
    }

    @Test
    public void testRunWhenNoEventsAvailable() {
        final List<StandardEvent> mockEvents = new ArrayList<>();

        MockListenUDP mockListenUDP = new MockListenUDP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenUDP);
        runner.setProperty(ListenRELP.PORT, "1");
        runner.setProperty(ListenRELP.MAX_BATCH_SIZE, "10");

        runner.run(5);
        runner.assertAllFlowFilesTransferred(ListenRELP.REL_SUCCESS, 0);
    }

    @Test
    public void testWithSendingHostAndPortSameAsSender() throws IOException, InterruptedException {
        final String sendingHost = "localhost";
        final Integer sendingPort = 21001;
        runner.setProperty(ListenUDP.SENDING_HOST, sendingHost);
        runner.setProperty(ListenUDP.SENDING_HOST_PORT, String.valueOf(sendingPort));

        // bind to the same sending port that processor has for Sending Host Port
        final DatagramSocket socket = new DatagramSocket(sendingPort);

        final List<String> messages = getMessages(6);
        final int expectedQueued = messages.size();
        final int expectedTransferred = messages.size();

        run(socket, messages, expectedQueued, expectedTransferred);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, messages.size());

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenUDP.REL_SUCCESS);
        verifyFlowFiles(mockFlowFiles);
        verifyProvenance(expectedTransferred);
    }

    private List<String> getMessages(int numMessages) {
        final List<String> messages = new ArrayList<>();
        for (int i=0; i < numMessages; i++) {
            messages.add("This is message " + (i + 1));
        }
        return messages;
    }

    private void verifyFlowFiles(List<MockFlowFile> mockFlowFiles) {
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            MockFlowFile flowFile = mockFlowFiles.get(i);
            flowFile.assertContentEquals("This is message " + (i + 1));
            Assert.assertEquals(String.valueOf(port), flowFile.getAttribute(ListenUDP.UDP_PORT_ATTR));
            Assert.assertTrue(StringUtils.isNotEmpty(flowFile.getAttribute(ListenUDP.UDP_SENDER_ATTR)));
        }
    }

    private void verifyProvenance(int expectedNumEvents) {
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        Assert.assertEquals(expectedNumEvents, provEvents.size());

        for (ProvenanceEventRecord event : provEvents) {
            Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
            Assert.assertTrue(event.getTransitUri().startsWith("udp://"));
        }
    }

    protected void run(final DatagramSocket socket, final List<String> messages, final int expectedQueueSize, final int expectedTransferred)
            throws IOException, InterruptedException {



        // Run Processor and start Dispatcher without shutting down
        runner.run(1, false, true);

        try {
            final InetSocketAddress destination = new InetSocketAddress(LOCALHOST, port);
            for (final String message : messages) {
                final byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
                final DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destination);
                socket.send(packet);
            }

            // Run Processor for number of responses
            runner.run(expectedTransferred, false, false);

            runner.assertTransferCount(ListenUDP.REL_SUCCESS, expectedTransferred);
        } finally {
            runner.shutdown();
        }
    }

    // Extend ListenUDP to mock the ChannelDispatcher and allow us to return staged events
    private static class MockListenUDP extends ListenUDP {

        private List<StandardEvent> mockEvents;

        public MockListenUDP(List<StandardEvent> mockEvents) {
            this.mockEvents = mockEvents;
        }

        @OnScheduled
        @Override
        public void onScheduled(ProcessContext context) throws IOException {
            super.onScheduled(context);
            events.addAll(mockEvents);
        }

        @Override
        protected ChannelDispatcher createDispatcher(ProcessContext context, BlockingQueue<StandardEvent> events) {
            return Mockito.mock(ChannelDispatcher.class);
        }

    }

}
