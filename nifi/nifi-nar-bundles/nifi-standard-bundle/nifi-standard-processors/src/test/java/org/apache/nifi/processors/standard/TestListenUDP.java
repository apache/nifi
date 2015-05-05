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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class TestListenUDP {

    private TestRunner runner;
    private static Logger LOGGER;
    private DatagramSocket socket;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ListenUDP", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestListenUDP", "debug");
        LOGGER = LoggerFactory.getLogger(TestListenUDP.class);
    }

    @AfterClass
    public static void tearDownAfterClass() {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "false");
    }

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ListenUDP.class);
        socket = new DatagramSocket(20001);
    }

    @After
    public void tearDown() throws Exception {
        socket.close();
    }

    @Test
    public void testWithStopProcessor() throws IOException, InterruptedException {
        LOGGER.info("Running testWithStopProcessor....");
        runner.setProperty(ListenUDP.PORT, "20000");
        runner.setProperty(ListenUDP.FLOW_FILE_SIZE_TRIGGER, "1 MB");
        runner.setProperty(ListenUDP.FLOW_FILES_PER_SESSION, "50");
        runner.setProperty(ListenUDP.CHANNEL_READER_PERIOD, "1 ms");
        runner.setProperty(ListenUDP.SENDING_HOST, "localhost");
        runner.setProperty(ListenUDP.SENDING_HOST_PORT, "20001");
        runner.setProperty(ListenUDP.RECV_TIMEOUT, "1 sec");
        Thread udpSender = new Thread(new DatagramSender(socket, false));

        ProcessContext context = runner.getProcessContext();
        ListenUDP processor = (ListenUDP) runner.getProcessor();
        ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        processor.initializeChannelListenerAndConsumerProcessing(context);
        udpSender.start();
        boolean transferred = false;
        long timeOut = System.currentTimeMillis() + 30000;
        while (!transferred && System.currentTimeMillis() < timeOut) {
            Thread.sleep(200);
            processor.onTrigger(context, processSessionFactory);
            transferred = runner.getFlowFilesForRelationship(ListenUDP.RELATIONSHIP_SUCCESS).size() > 0;
        }
        assertTrue("Didn't process the datagrams", transferred);
        Thread.sleep(7000);
        processor.stopping();
        processor.stopped();
        socket.close();
        assertTrue(runner.getFlowFilesForRelationship(ListenUDP.RELATIONSHIP_SUCCESS).size() >= 60);
    }

    @Test
    public void testWithSlowRate() throws IOException, InterruptedException {
        LOGGER.info("Running testWithSlowRate....");
        runner.setProperty(ListenUDP.PORT, "20000");
        runner.setProperty(ListenUDP.FLOW_FILE_SIZE_TRIGGER, "1 B");
        runner.setProperty(ListenUDP.FLOW_FILES_PER_SESSION, "1");
        runner.setProperty(ListenUDP.CHANNEL_READER_PERIOD, "50 ms");
        runner.setProperty(ListenUDP.MAX_BUFFER_SIZE, "2 MB");
        runner.setProperty(ListenUDP.SENDING_HOST, "localhost");
        runner.setProperty(ListenUDP.SENDING_HOST_PORT, "20001");
        runner.setProperty(ListenUDP.RECV_TIMEOUT, "5 sec");
        final DatagramSender sender = new DatagramSender(socket, false);
        sender.throttle = 5000;
        sender.numpackets = 10;
        Thread udpSender = new Thread(sender);

        ProcessContext context = runner.getProcessContext();
        ListenUDP processor = (ListenUDP) runner.getProcessor();
        ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        processor.initializeChannelListenerAndConsumerProcessing(context);
        udpSender.start();
        boolean transferred = false;
        long timeOut = System.currentTimeMillis() + 60000;
        while (!transferred && System.currentTimeMillis() < timeOut) {
            Thread.sleep(1000);
            processor.onTrigger(context, processSessionFactory);
            transferred = runner.getFlowFilesForRelationship(ListenUDP.RELATIONSHIP_SUCCESS).size() > 0;
        }
        assertTrue("Didn't process the datagrams", transferred);
        Thread.sleep(7000);
        processor.stopping();
        processor.stopped();
        socket.close();
        assertTrue(runner.getFlowFilesForRelationship(ListenUDP.RELATIONSHIP_SUCCESS).size() >= 2);
    }

    @Test
    public void testWithCloseSenderAndNoStopping() throws Exception {
        LOGGER.info("Running testWithCloseSenderAndNoStopping....");
        runner.setProperty(ListenUDP.PORT, "20000");
        runner.setProperty(ListenUDP.FLOW_FILE_SIZE_TRIGGER, "1 MB");
        runner.setProperty(ListenUDP.FLOW_FILES_PER_SESSION, "50");
        runner.setProperty(ListenUDP.CHANNEL_READER_PERIOD, "1 ms");
        runner.setProperty(ListenUDP.SENDING_HOST, "localhost");
        runner.setProperty(ListenUDP.SENDING_HOST_PORT, "20001");
        runner.setProperty(ListenUDP.RECV_TIMEOUT, "1 sec");
        Thread udpSender = new Thread(new DatagramSender(socket, false));

        ProcessContext context = runner.getProcessContext();
        ListenUDP processor = (ListenUDP) runner.getProcessor();
        ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        processor.initializeChannelListenerAndConsumerProcessing(context);
        udpSender.start();
        int numTransfered = 0;
        long timeout = System.currentTimeMillis() + 22000;
        while (numTransfered <= 80 && System.currentTimeMillis() < timeout) {
            Thread.sleep(200);
            processor.onTrigger(context, processSessionFactory);
            numTransfered = runner.getFlowFilesForRelationship(ListenUDP.RELATIONSHIP_SUCCESS).size();
        }
        assertFalse("Did not process all the datagrams", numTransfered < 80);
        processor.stopping();
        processor.stopped();
        socket.close();
    }

    private static final class DatagramSender implements Runnable {

        private final DatagramSocket socket;
        private final boolean closeSocket;
        long throttle = 0;
        int numpackets = 819200;

        private DatagramSender(DatagramSocket socket, boolean closeSocket) {
            this.socket = socket;
            this.closeSocket = closeSocket;
        }

        @Override
        public void run() {
            final byte[] buffer = new byte[128];
            try {
                final DatagramPacket packet = new DatagramPacket(buffer, buffer.length, new InetSocketAddress("localhost", 20000));
                final long startTime = System.nanoTime();
                for (int i = 0; i < numpackets; i++) { // 100 MB
                    socket.send(packet);
                    if (throttle > 0) {
                        try {
                            Thread.sleep(throttle);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                final long endTime = System.nanoTime();
                final long durationMillis = (endTime - startTime) / 1000000;
                LOGGER.info("Sent all UDP packets without any obvious errors | duration ms= " + durationMillis);
            } catch (IOException e) {
                LOGGER.error("", e);
            } finally {
                if (closeSocket) {
                    socket.close();
                    LOGGER.info("Socket closed");
                }
            }
        }
    }
}
