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
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestListenUDP {

    private static final String LOCALHOST = "localhost";


    private TestRunner runner;

    @Mock
    private ChannelResponder<DatagramChannel> responder;

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ListenUDP.class);
        runner.setProperty(ListenUDP.PORT, "0");
    }

    @Test
    public void testCustomValidation() {
        runner.setProperty(ListenUDP.PORT, "1");
        runner.assertValid();

        runner.setProperty(ListenUDP.SENDING_HOST, LOCALHOST);
        runner.assertNotValid();

        runner.setProperty(ListenUDP.SENDING_HOST_PORT, "1234");
        runner.assertValid();

        runner.setProperty(ListenUDP.SENDING_HOST, "");
        runner.assertNotValid();
    }

    @Test
    public void testDefaultBehavior() throws IOException, InterruptedException {
        final List<String> messages = getMessages(15);
        final int expectedTransferred = messages.size();

        run(new DatagramSocket(), messages, expectedTransferred);
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

        run(new DatagramSocket(), messages, maxQueueSize);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, maxQueueSize);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenUDP.REL_SUCCESS);
        verifyFlowFiles(mockFlowFiles);
        verifyProvenance(maxQueueSize);
    }

    @Test
    public void testBatchingSingleSender() throws IOException, InterruptedException {
        final String delimiter = "NN";
        runner.setProperty(ListenerProperties.MESSAGE_DELIMITER, delimiter);
        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "3");

        final List<String> messages = getMessages(5);
        final int expectedTransferred = 2;

        run(new DatagramSocket(), messages, expectedTransferred);
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
        final byte[] message = "test message".getBytes(StandardCharsets.UTF_8);

        final List<StandardEvent<DatagramChannel>> mockEvents = new ArrayList<>();
        mockEvents.add(new StandardEvent<>(sender1, message, responder));
        mockEvents.add(new StandardEvent<>(sender1, message, responder));
        mockEvents.add(new StandardEvent<>(sender2, message, responder));
        mockEvents.add(new StandardEvent<>(sender2, message, responder));

        MockListenUDP mockListenUDP = new MockListenUDP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenUDP);
        runner.setProperty(ListenUDP.PORT, "1");
        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "10");

        // sending 4 messages with a batch size of 10, but should get 2 FlowFiles because of different senders
        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, 2);

        verifyProvenance(2);
    }

    @Test
    public void testRunWhenNoEventsAvailable() {
        final List<StandardEvent<DatagramChannel>> mockEvents = new ArrayList<>();

        MockListenUDP mockListenUDP = new MockListenUDP(mockEvents);
        runner = TestRunners.newTestRunner(mockListenUDP);
        runner.setProperty(ListenUDP.PORT, "1");
        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "10");

        runner.run(5);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, 0);
    }

    @Test
    public void testWithSendingHostAndPortSameAsSender() throws IOException, InterruptedException {
        // bind to the same sending port that processor has for Sending Host Port
        final DatagramSocket socket = new DatagramSocket();
        final int sendingPort = socket.getLocalPort();

        runner.setProperty(ListenUDP.SENDING_HOST, LOCALHOST);
        runner.setProperty(ListenUDP.SENDING_HOST_PORT, String.valueOf(sendingPort));

        final List<String> messages = getMessages(6);
        final int expectedTransferred = messages.size();

        run(socket, messages, expectedTransferred);
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
        final int port = ((ListenUDP) runner.getProcessor()).getListeningPort();

        for (int i = 0; i < mockFlowFiles.size(); i++) {
            MockFlowFile flowFile = mockFlowFiles.get(i);
            flowFile.assertContentEquals("This is message " + (i + 1));
            assertEquals(String.valueOf(port), flowFile.getAttribute(ListenUDP.UDP_PORT_ATTR));
            assertTrue(StringUtils.isNotEmpty(flowFile.getAttribute(ListenUDP.UDP_SENDER_ATTR)));
        }
    }

    private void verifyProvenance(int expectedNumEvents) {
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertEquals(expectedNumEvents, provEvents.size());

        for (ProvenanceEventRecord event : provEvents) {
            assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
            assertTrue(event.getTransitUri().startsWith("udp://"));
        }
    }

    protected void run(final DatagramSocket socket, final List<String> messages, final int expectedTransferred)
            throws IOException, InterruptedException {
        // Run Processor and start Dispatcher without shutting down
        runner.run(1, false, true);
        final int port = ((ListenUDP) runner.getProcessor()).getListeningPort();

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

        private final List<StandardEvent<DatagramChannel>> mockEvents;

        public MockListenUDP(List<StandardEvent<DatagramChannel>> mockEvents) {
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
