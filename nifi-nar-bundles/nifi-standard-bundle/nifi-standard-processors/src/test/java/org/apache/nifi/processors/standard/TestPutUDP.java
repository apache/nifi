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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestPutUDP {

    private final static String UDP_SERVER_ADDRESS = "127.0.0.1";
    private final static String SERVER_VARIABLE = "ALKJAFLKJDFLSKJSDFLKJSDF";
    private final static String UDP_SERVER_ADDRESS_EL = "${" + SERVER_VARIABLE + "}";
    private final static String UNKNOWN_HOST = "fgdsfgsdffd";
    private final static String INVALID_IP_ADDRESS = "300.300.300.300";
    private static final String DELIMITER = "\n";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final static int MAX_FRAME_LENGTH = 32800;
    private final static int VALID_LARGE_FILE_SIZE = 32768;
    private final static int VALID_SMALL_FILE_SIZE = 64;
    private final static int INVALID_LARGE_FILE_SIZE = 1000000;
    private final static int LOAD_TEST_ITERATIONS = 500;
    private final static int LOAD_TEST_THREAD_COUNT = 1;
    private final static int DEFAULT_ITERATIONS = 1;
    private final static int DEFAULT_THREAD_COUNT = 1;
    private final static char CONTENT_CHAR = 'x';
    private final static int DATA_WAIT_PERIOD = 1000;
    private final static int DEFAULT_TEST_TIMEOUT_PERIOD = 10000;
    private final static int LONG_TEST_TIMEOUT_PERIOD = 100000;

    private TestRunner runner;
    private int port;
    private TransportProtocol PROTOCOL = TransportProtocol.UDP;
    private EventServer eventServer;
    private BlockingQueue<ByteArrayMessage> messages;


    // Test Data
    private final static String[] EMPTY_FILE = { "" };
    private final static String[] VALID_FILES = { "abcdefghijklmnopqrstuvwxyz", "zyxwvutsrqponmlkjihgfedcba", "12345678", "343424222", "!@£$%^&*()_+:|{}[];\\" };

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(PutUDP.class);
        runner.setVariable(SERVER_VARIABLE, UDP_SERVER_ADDRESS);
        port = NetworkUtils.getAvailableUdpPort();
        createTestServer(UDP_SERVER_ADDRESS, port, VALID_LARGE_FILE_SIZE);
    }

    private void createTestServer(final String address, final int port, final int frameSize) throws Exception {
        messages = new LinkedBlockingQueue<>();
        final byte[] delimiter = DELIMITER.getBytes(CHARSET);
        NettyEventServerFactory serverFactory = new ByteArrayMessageNettyEventServerFactory(runner.getLogger(), address, port, PROTOCOL, delimiter, frameSize, messages);
        serverFactory.setSocketReceiveBuffer(MAX_FRAME_LENGTH);
        eventServer = serverFactory.getEventServer();
    }

    @After
    public void cleanup() throws Exception {
        runner.shutdown();
        removeTestServer();
    }

    private void removeTestServer() {
        if (eventServer != null) {
            eventServer.shutdown();
            eventServer = null;
        }
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testValidFiles() throws Exception {
        configureProperties(UDP_SERVER_ADDRESS, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(VALID_FILES);
        checkInputQueueIsEmpty();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testValidFilesEL() throws Exception {
        configureProperties(UDP_SERVER_ADDRESS_EL, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(VALID_FILES);
        checkInputQueueIsEmpty();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testEmptyFile() throws Exception {
        configureProperties(UDP_SERVER_ADDRESS, true);
        sendTestData(EMPTY_FILE);
        checkRelationships(EMPTY_FILE.length, 0);
        checkNoDataReceived();
        checkInputQueueIsEmpty();
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testLargeValidFile() throws Exception {
        configureProperties(UDP_SERVER_ADDRESS, true);
        final String[] testData = createContent(VALID_LARGE_FILE_SIZE);
        sendTestData(testData);
        checkReceivedAllData(testData);
        checkInputQueueIsEmpty();
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testLargeInvalidFile() throws Exception {
        configureProperties(UDP_SERVER_ADDRESS, true);
        String[] testData = createContent(INVALID_LARGE_FILE_SIZE);
        sendTestData(testData);
        checkRelationships(0, testData.length);
        checkNoDataReceived();
        checkInputQueueIsEmpty();
    }

    @Ignore("This test is failing intermittently as documented in NIFI-4288")
    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testInvalidIPAddress() throws Exception {
        configureProperties(INVALID_IP_ADDRESS, true);
        sendTestData(VALID_FILES);
        checkNoDataReceived();
        checkRelationships(0, VALID_FILES.length);
        checkInputQueueIsEmpty();
    }

    @Ignore("This test is failing incorrectly as documented in NIFI-1795")
    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testUnknownHostname() throws Exception {
        configureProperties(UNKNOWN_HOST, true);
        sendTestData(VALID_FILES);
        checkNoDataReceived();
        checkRelationships(0, VALID_FILES.length);
        checkInputQueueIsEmpty();
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testReconfiguration() throws Exception {
        configureProperties(UDP_SERVER_ADDRESS, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(VALID_FILES);
        reset(UDP_SERVER_ADDRESS, port, MAX_FRAME_LENGTH);
        configureProperties(UDP_SERVER_ADDRESS, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(VALID_FILES);
        reset(UDP_SERVER_ADDRESS, port, MAX_FRAME_LENGTH);
        configureProperties(UDP_SERVER_ADDRESS, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(VALID_FILES);
        checkInputQueueIsEmpty();
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testLoadTest() throws Exception {
        final String[] testData = createContent(VALID_SMALL_FILE_SIZE);
        configureProperties(UDP_SERVER_ADDRESS, true);
        sendTestData(testData, LOAD_TEST_ITERATIONS, LOAD_TEST_THREAD_COUNT);
        checkReceivedAllData(testData, LOAD_TEST_ITERATIONS);
        checkInputQueueIsEmpty();
    }

    private void reset(final String address, final int port, final int frameSize) throws Exception {
        runner.clearTransferState();
        removeTestServer();
        createTestServer(address, port, frameSize);
    }

    private void configureProperties(final String host, final boolean expectValid) {
        runner.setProperty(PutUDP.HOSTNAME, host);
        runner.setProperty(PutUDP.PORT, Integer.toString(port));
        runner.setProperty(PutUDP.MAX_SOCKET_SEND_BUFFER_SIZE, "40000B");

        if (expectValid) {
            runner.assertValid();
        } else {
            runner.assertNotValid();
        }
    }

    private void sendTestData(final String[] testData) throws InterruptedException {
        sendTestData(testData, DEFAULT_ITERATIONS, DEFAULT_THREAD_COUNT);
    }

    private void sendTestData(final String[] testData, final int iterations, final int threadCount) throws InterruptedException {
        for (String item : testData) {
            runner.setThreadCount(threadCount);
            for (int i = 0; i < iterations; i++) {
                runner.enqueue(item.getBytes());
                runner.run(1, false);
                Thread.sleep(1);
            }
        }

        // ensure @OnStopped methods get called
        runner.run();
    }

    private void checkRelationships(final int successCount, final int failedCount) {
        runner.assertTransferCount(PutUDP.REL_SUCCESS, successCount);
        runner.assertTransferCount(PutUDP.REL_FAILURE, failedCount);
    }

    private void checkNoDataReceived() throws Exception {
        Thread.sleep(DATA_WAIT_PERIOD);
        assertNull("Unexpected extra messages found", messages.poll());
    }

    private void checkInputQueueIsEmpty() {
        runner.assertQueueEmpty();
    }

    private void checkReceivedAllData(final String[] sentData) throws Exception {
        checkReceivedAllData(sentData, DEFAULT_ITERATIONS);
    }

    private void checkReceivedAllData(final String[] sentData, final int iterations) throws Exception {
        // check each sent FlowFile was successfully sent and received.
         for (String item : sentData) {
            for (int i = 0; i < iterations; i++) {
                ByteArrayMessage packet = messages.take();
                assertNotNull(packet);
                assertArrayEquals(item.getBytes(), packet.getMessage());
            }
        }

        runner.assertTransferCount(PutUDP.REL_SUCCESS, sentData.length * iterations);

        assertNull("Unexpected extra messages found", messages.poll());
    }

    private String[] createContent(final int size) {
        final char[] content = new char[size];

        for (int i = 0; i < size; i++) {
            content[i] = CONTENT_CHAR;
        }

        return new String[] { new String(content).concat("\n") };
    }
}
