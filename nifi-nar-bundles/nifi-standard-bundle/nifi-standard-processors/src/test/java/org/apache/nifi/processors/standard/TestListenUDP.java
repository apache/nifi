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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

    private int port = 0;
    private ListenUDP proc;
    private TestRunner runner;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ListenUDP", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestListenUDP", "debug");
    }

    @AfterClass
    public static void tearDownAfterClass() {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "false");
    }

    @Before
    public void setUp() throws Exception {
        proc = new ListenUDP();
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenUDP.PORT, String.valueOf(port));
    }

    @Test
    public void testCustomValidation() {
        runner.assertNotValid();
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
        final int expectedQueued = maxQueueSize;
        final int expectedTransferred = maxQueueSize;

        run(new DatagramSocket(), messages, expectedQueued, expectedTransferred);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, maxQueueSize);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenUDP.REL_SUCCESS);
        verifyFlowFiles(mockFlowFiles);
        verifyProvenance(expectedTransferred);
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
    public void testBatchingWithDifferentSenders() throws IOException, InterruptedException {
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
    public void testRunWhenNoEventsAvailable() throws IOException, InterruptedException {
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

    @Test
    public void testWithSendingHostAndPortDifferentThanSender() throws IOException, InterruptedException {
        final String sendingHost = "localhost";
        final Integer sendingPort = 21001;
        runner.setProperty(ListenUDP.SENDING_HOST, sendingHost);
        runner.setProperty(ListenUDP.SENDING_HOST_PORT, String.valueOf(sendingPort));

        // bind to a different sending port than the processor has for Sending Host Port
        final DatagramSocket socket = new DatagramSocket(21002);

        // no messages should come through since we are listening for 21001 and sending from 21002

        final List<String> messages = getMessages(6);
        final int expectedQueued = 0;
        final int expectedTransferred = 0;

        run(socket, messages, expectedQueued, expectedTransferred);
        runner.assertAllFlowFilesTransferred(ListenUDP.REL_SUCCESS, 0);
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

        try {
            // schedule to start listening on a random port
            final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
            final ProcessContext context = runner.getProcessContext();
            proc.onScheduled(context);
            Thread.sleep(100);

            // get the real port the dispatcher is listening on
            final int destPort = proc.getDispatcherPort();
            final InetSocketAddress destination = new InetSocketAddress("localhost", destPort);

            // send the messages to the port the processors is listening on
            for (final String message : messages) {
                final byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
                final DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destination);
                socket.send(packet);
                Thread.sleep(10);
            }

            long responseTimeout = 10000;

            // this first loop waits until the internal queue of the processor has the expected
            // number of messages ready before proceeding, we want to guarantee they are all there
            // before onTrigger gets a chance to run
            long startTimeQueueSizeCheck = System.currentTimeMillis();
            while (proc.getQueueSize() < expectedQueueSize
                    && (System.currentTimeMillis() - startTimeQueueSizeCheck < responseTimeout)) {
                Thread.sleep(100);
            }

            // want to fail here if the queue size isn't what we expect
            Assert.assertEquals(expectedQueueSize, proc.getQueueSize());

            // call onTrigger until we processed all the messages, or a certain amount of time passes
            int numTransferred = 0;
            long startTime = System.currentTimeMillis();
            while (numTransferred < expectedTransferred  && (System.currentTimeMillis() - startTime < responseTimeout)) {
                proc.onTrigger(context, processSessionFactory);
                numTransferred = runner.getFlowFilesForRelationship(ListenUDP.REL_SUCCESS).size();
                Thread.sleep(100);
            }

            // should have transferred the expected events
            runner.assertTransferCount(ListenUDP.REL_SUCCESS, expectedTransferred);
        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
            IOUtils.closeQuietly(socket);
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
        protected ChannelDispatcher createDispatcher(ProcessContext context, BlockingQueue<StandardEvent> events) throws IOException {
            return Mockito.mock(ChannelDispatcher.class);
        }

    }

}
