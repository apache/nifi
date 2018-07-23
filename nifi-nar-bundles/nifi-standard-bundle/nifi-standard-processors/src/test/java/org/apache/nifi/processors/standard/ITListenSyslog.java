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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.nifi.processors.standard.TestListenSyslog.DatagramSender;

public class ITListenSyslog {

    static final Logger LOGGER = LoggerFactory.getLogger(ITListenSyslog.class);

    static final String PRI = "34";
    static final String SEV = "2";
    static final String FAC = "4";
    static final String TIME = "Oct 13 15:43:23";
    static final String HOST = "localhost.home";
    static final String BODY = "some message";

    static final String VALID_MESSAGE = "<" + PRI + ">" + TIME + " " + HOST + " " + BODY;
    static final String VALID_MESSAGE_TCP = "<" + PRI + ">" + TIME + " " + HOST + " " + BODY + "\n";
    static final String INVALID_MESSAGE = "this is not valid\n";

    @Test
    public void testUDP() throws IOException, InterruptedException {
        final ListenSyslog proc = new ListenSyslog();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenSyslog.PROTOCOL, ListenSyslog.UDP_VALUE.getValue());
        runner.setProperty(ListenSyslog.PORT, "0");

        // schedule to start listening on a random port
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.onScheduled(context);

        final int numMessages = 20;
        final int port = proc.getPort();
        Assert.assertTrue(port > 0);

        // write some UDP messages to the port in the background
        final Thread sender = new Thread(new DatagramSender(port, numMessages, 10, VALID_MESSAGE));
        sender.setDaemon(true);
        sender.start();

        // call onTrigger until we read all datagrams, or 30 seconds passed
        try {
            int numTransferred = 0;
            long timeout = System.currentTimeMillis() + 30000;

            while (numTransferred < numMessages && System.currentTimeMillis() < timeout) {
                Thread.sleep(10);
                proc.onTrigger(context, processSessionFactory);
                numTransferred = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).size();
            }
            Assert.assertEquals("Did not process all the datagrams", numMessages, numTransferred);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).get(0);
            checkFlowFile(flowFile, 0, ListenSyslog.UDP_VALUE.getValue());

            final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
            Assert.assertNotNull(events);
            Assert.assertEquals(numMessages, events.size());

            final ProvenanceEventRecord event = events.get(0);
            Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
            Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("udp"));

        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
        }
    }

    @Test
    public void testTCPSingleConnection() throws IOException, InterruptedException {
        final ListenSyslog proc = new ListenSyslog();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenSyslog.PROTOCOL, ListenSyslog.TCP_VALUE.getValue());
        runner.setProperty(ListenSyslog.PORT, "0");

        // schedule to start listening on a random port
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.onScheduled(context);

        // Allow time for the processor to perform its scheduled start
        Thread.sleep(500);

        final int numMessages = 20;
        final int port = proc.getPort();
        Assert.assertTrue(port > 0);

        // write some TCP messages to the port in the background
        final Thread sender = new Thread(new SingleConnectionSocketSender(port, numMessages, 10, VALID_MESSAGE_TCP));
        sender.setDaemon(true);
        sender.start();

        // call onTrigger until we read all messages, or 30 seconds passed
        try {
            int nubTransferred = 0;
            long timeout = System.currentTimeMillis() + 30000;

            while (nubTransferred < numMessages && System.currentTimeMillis() < timeout) {
                Thread.sleep(10);
                proc.onTrigger(context, processSessionFactory);
                nubTransferred = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).size();
            }
            Assert.assertEquals("Did not process all the messages", numMessages, nubTransferred);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).get(0);
            checkFlowFile(flowFile, 0, ListenSyslog.TCP_VALUE.getValue());

            final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
            Assert.assertNotNull(events);
            Assert.assertEquals(numMessages, events.size());

            final ProvenanceEventRecord event = events.get(0);
            Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
            Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("tcp"));

        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
        }
    }

    @Test
    public void testTCPSingleConnectionWithNewLines() throws IOException, InterruptedException {
        final ListenSyslog proc = new ListenSyslog();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenSyslog.PROTOCOL, ListenSyslog.TCP_VALUE.getValue());
        runner.setProperty(ListenSyslog.PORT, "0");

        // schedule to start listening on a random port
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.onScheduled(context);

        final int numMessages = 3;
        final int port = proc.getPort();
        Assert.assertTrue(port > 0);

        // send 3 messages as 1
        final String multipleMessages = VALID_MESSAGE_TCP + "\n" + VALID_MESSAGE_TCP + "\n" + VALID_MESSAGE_TCP + "\n";
        final Thread sender = new Thread(new SingleConnectionSocketSender(port, 1, 10, multipleMessages));
        sender.setDaemon(true);
        sender.start();

        // call onTrigger until we read all messages, or 30 seconds passed
        try {
            int nubTransferred = 0;
            long timeout = System.currentTimeMillis() + 30000;

            while (nubTransferred < numMessages && System.currentTimeMillis() < timeout) {
                Thread.sleep(10);
                proc.onTrigger(context, processSessionFactory);
                nubTransferred = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).size();
            }
            Assert.assertEquals("Did not process all the messages", numMessages, nubTransferred);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).get(0);
            checkFlowFile(flowFile, 0, ListenSyslog.TCP_VALUE.getValue());

            final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
            Assert.assertNotNull(events);
            Assert.assertEquals(numMessages, events.size());

            final ProvenanceEventRecord event = events.get(0);
            Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
            Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("tcp"));

        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
        }
    }

    @Test
    public void testTCPMultipleConnection() throws IOException, InterruptedException {
        final ListenSyslog proc = new ListenSyslog();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenSyslog.PROTOCOL, ListenSyslog.TCP_VALUE.getValue());
        runner.setProperty(ListenSyslog.MAX_CONNECTIONS, "5");
        runner.setProperty(ListenSyslog.PORT, "0");

        // schedule to start listening on a random port
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.onScheduled(context);

        final int numMessages = 20;
        final int port = proc.getPort();
        Assert.assertTrue(port > 0);

        // write some TCP messages to the port in the background
        final Thread sender = new Thread(new MultiConnectionSocketSender(port, numMessages, 10, VALID_MESSAGE_TCP));
        sender.setDaemon(true);
        sender.start();

        // call onTrigger until we read all messages, or 30 seconds passed
        try {
            int nubTransferred = 0;
            long timeout = System.currentTimeMillis() + 30000;

            while (nubTransferred < numMessages && System.currentTimeMillis() < timeout) {
                Thread.sleep(10);
                proc.onTrigger(context, processSessionFactory);
                nubTransferred = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).size();
            }
            Assert.assertEquals("Did not process all the messages", numMessages, nubTransferred);

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).get(0);
            checkFlowFile(flowFile, 0, ListenSyslog.TCP_VALUE.getValue());

            final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
            Assert.assertNotNull(events);
            Assert.assertEquals(numMessages, events.size());

            final ProvenanceEventRecord event = events.get(0);
            Assert.assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());
            Assert.assertTrue("transit uri must be set and start with proper protocol", event.getTransitUri().toLowerCase().startsWith("tcp"));

        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
        }
    }

    @Test
    public void testInvalid() throws IOException, InterruptedException {
        final ListenSyslog proc = new ListenSyslog();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenSyslog.PROTOCOL, ListenSyslog.TCP_VALUE.getValue());
        runner.setProperty(ListenSyslog.PORT, "0");

        // schedule to start listening on a random port
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.onScheduled(context);

        final int numMessages = 10;
        final int port = proc.getPort();
        Assert.assertTrue(port > 0);

        // write some TCP messages to the port in the background
        final Thread sender = new Thread(new SingleConnectionSocketSender(port, numMessages, 100, INVALID_MESSAGE));
        sender.setDaemon(true);
        sender.start();

        // call onTrigger until we read all messages, or 30 seconds passed
        try {
            int nubTransferred = 0;
            long timeout = System.currentTimeMillis() + 30000;

            while (nubTransferred < numMessages && System.currentTimeMillis() < timeout) {
                Thread.sleep(50);
                proc.onTrigger(context, processSessionFactory);
                nubTransferred = runner.getFlowFilesForRelationship(ListenSyslog.REL_INVALID).size();
            }

            // all messages should be transferred to invalid
            Assert.assertEquals("Did not process all the messages", numMessages, nubTransferred);

        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
        }
    }

    private void checkFlowFile(final MockFlowFile flowFile, final int port, final String protocol) {
        flowFile.assertContentEquals(VALID_MESSAGE.replace("\n", ""));
        Assert.assertEquals(PRI, flowFile.getAttribute(SyslogAttributes.SYSLOG_PRIORITY.key()));
        Assert.assertEquals(SEV, flowFile.getAttribute(SyslogAttributes.SYSLOG_SEVERITY.key()));
        Assert.assertEquals(FAC, flowFile.getAttribute(SyslogAttributes.SYSLOG_FACILITY.key()));
        Assert.assertEquals(TIME, flowFile.getAttribute(SyslogAttributes.SYSLOG_TIMESTAMP.key()));
        Assert.assertEquals(HOST, flowFile.getAttribute(SyslogAttributes.SYSLOG_HOSTNAME.key()));
        Assert.assertEquals(BODY, flowFile.getAttribute(SyslogAttributes.SYSLOG_BODY.key()));
        Assert.assertEquals("true", flowFile.getAttribute(SyslogAttributes.SYSLOG_VALID.key()));
        Assert.assertEquals(String.valueOf(port), flowFile.getAttribute(SyslogAttributes.SYSLOG_PORT.key()));
        Assert.assertEquals(protocol, flowFile.getAttribute(SyslogAttributes.SYSLOG_PROTOCOL.key()));
        Assert.assertTrue(!StringUtils.isBlank(flowFile.getAttribute(SyslogAttributes.SYSLOG_SENDER.key())));
    }

    /**
     * Sends a given number of datagrams to the given port.
     */
    public static final class SingleConnectionSocketSender implements Runnable {

        final int port;
        final int numMessages;
        final long delay;
        final String message;

        public SingleConnectionSocketSender(int port, int numMessages, long delay, String message) {
            this.port = port;
            this.numMessages = numMessages;
            this.delay = delay;
            this.message = message;
        }

        @Override
        public void run() {
            byte[] bytes = message.getBytes(Charset.forName("UTF-8"));
            final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);

            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(new InetSocketAddress("localhost", port));

                for (int i = 0; i < numMessages; i++) {
                    buffer.clear();
                    buffer.put(bytes);
                    buffer.flip();

                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    Thread.sleep(delay);
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Sends a given number of datagrams to the given port.
     */
    public static final class MultiConnectionSocketSender implements Runnable {

        final int port;
        final int numMessages;
        final long delay;
        final String message;

        public MultiConnectionSocketSender(int port, int numMessages, long delay, String message) {
            this.port = port;
            this.numMessages = numMessages;
            this.delay = delay;
            this.message = message;
        }

        @Override
        public void run() {
            byte[] bytes = message.getBytes(Charset.forName("UTF-8"));
            final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);

            for (int i = 0; i < numMessages; i++) {
                try (SocketChannel channel = SocketChannel.open()) {
                    channel.connect(new InetSocketAddress("localhost", port));

                    buffer.clear();
                    buffer.put(bytes);
                    buffer.flip();

                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    Thread.sleep(delay);
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

}
