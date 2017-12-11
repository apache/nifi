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

package org.apache.nifi.processors.standard.util;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.processors.standard.PutTCP;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public abstract class TestPutTCPCommon {
    private final static String TCP_SERVER_ADDRESS = "127.0.0.1";
    private final static String SERVER_VARIABLE = "ALKJAFLKJDFLSKJSDFLKJSDF";
    private final static String TCP_SERVER_ADDRESS_EL = "${" + SERVER_VARIABLE + "}";
    private final static String UNKNOWN_HOST = "fgdsfgsdffd";
    private final static String INVALID_IP_ADDRESS = "300.300.300.300";
    private final static int MIN_INVALID_PORT = 0;
    private final static int MIN_VALID_PORT = 1;
    private final static int MAX_VALID_PORT = 65535;
    private final static int MAX_INVALID_PORT = 65536;
    private final static int BUFFER_SIZE = 1024;
    private final static int VALID_LARGE_FILE_SIZE = 32768;
    private final static int VALID_SMALL_FILE_SIZE = 64;
    private final static int LOAD_TEST_ITERATIONS = 500;
    private final static int LOAD_TEST_THREAD_COUNT = 1;
    private final static int DEFAULT_ITERATIONS = 1;
    private final static int DEFAULT_THREAD_COUNT = 1;
    private final static char CONTENT_CHAR = 'x';
    private final static int DATA_WAIT_PERIOD = 1000;
    private final static int DEFAULT_TEST_TIMEOUT_PERIOD = 10000;
    private final static int LONG_TEST_TIMEOUT_PERIOD = 100000;
    private final static String OUTGOING_MESSAGE_DELIMITER = "\n";
    private final static String OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR = "{delimiter}\r\n";

    private TCPTestServer server;
    private int tcp_server_port;
    private ArrayBlockingQueue<List<Byte>> recvQueue;

    public boolean ssl;
    public TestRunner runner;

    // Test Data
    private final static String[] EMPTY_FILE = { "" };
    private final static String[] VALID_FILES = { "abcdefghijklmnopqrstuvwxyz", "zyxwvutsrqponmlkjihgfedcba", "12345678", "343424222", "!@£$%^&*()_+:|{}[];\\" };

    @Before
    public void setup() throws Exception {
        recvQueue = new ArrayBlockingQueue<List<Byte>>(BUFFER_SIZE);
        runner = TestRunners.newTestRunner(PutTCP.class);
        runner.setVariable(SERVER_VARIABLE, TCP_SERVER_ADDRESS);
    }

    private synchronized TCPTestServer createTestServer(final String address, final ArrayBlockingQueue<List<Byte>> recvQueue, final String delimiter) throws Exception {
        TCPTestServer server = new TCPTestServer(InetAddress.getByName(address), recvQueue, delimiter);
        server.startServer(ssl);
        tcp_server_port = server.getPort();
        return server;
    }

    @After
    public void cleanup() throws Exception {
        runner.shutdown();
        removeTestServer(server);
    }

    private void removeTestServer(TCPTestServer server) {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testValidFiles() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testValidFilesEL() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS_EL, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testPruneSenders() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(VALID_FILES);
        Thread.sleep(10);
        checkRelationships(VALID_FILES.length, 0);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
        runner.setProperty(PutTCP.IDLE_EXPIRATION, "1 second");
        Thread.sleep(2000);
        runner.run(1, false, false);
        runner.clearTransferState();
        sendTestData(VALID_FILES);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testMultiCharDelimiter() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR, false, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testConnectionPerFlowFile() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, true, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, VALID_FILES.length);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testConnectionFailure() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
        removeTestServer(server);
        runner.clearTransferState();
        sendTestData(VALID_FILES);
        Thread.sleep(10);
        checkNoDataReceived(recvQueue);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(VALID_FILES);
        checkReceivedAllData(recvQueue, VALID_FILES);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testEmptyFile() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(EMPTY_FILE);
        Thread.sleep(10);
        checkRelationships(EMPTY_FILE.length, 0);
        checkEmptyMessageReceived(recvQueue);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testlargeValidFile() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, true, true);
        final String[] testData = createContent(VALID_LARGE_FILE_SIZE);
        sendTestData(testData);
        checkReceivedAllData(recvQueue, testData);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, testData.length);
    }

    @Ignore("This test is failing intermittently as documented in NIFI-4288")
    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testInvalidIPAddress() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(INVALID_IP_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(VALID_FILES);
        Thread.sleep(10);
        checkRelationships(0, VALID_FILES.length);
        checkNoDataReceived(recvQueue);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 0);
    }

    @Ignore("This test is failing intermittently as documented in NIFI-4288")
    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testUnknownHostname() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(UNKNOWN_HOST, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(VALID_FILES);
        Thread.sleep(10);
        checkRelationships(0, VALID_FILES.length);
        checkNoDataReceived(recvQueue);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 0);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testInvalidPort() throws Exception {
        configureProperties(UNKNOWN_HOST, MIN_INVALID_PORT, OUTGOING_MESSAGE_DELIMITER, false, false);
        configureProperties(UNKNOWN_HOST, MIN_VALID_PORT, OUTGOING_MESSAGE_DELIMITER, false, true);
        configureProperties(UNKNOWN_HOST, MAX_VALID_PORT, OUTGOING_MESSAGE_DELIMITER, false, true);
        configureProperties(UNKNOWN_HOST, MAX_INVALID_PORT, OUTGOING_MESSAGE_DELIMITER, false, false);
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testLoadTest() throws Exception {
        server = createTestServer(TCP_SERVER_ADDRESS, recvQueue, OUTGOING_MESSAGE_DELIMITER);
        Thread.sleep(1000);
        final String[] testData = createContent(VALID_SMALL_FILE_SIZE);
        configureProperties(TCP_SERVER_ADDRESS, tcp_server_port, OUTGOING_MESSAGE_DELIMITER, false, true);
        sendTestData(testData, LOAD_TEST_ITERATIONS, LOAD_TEST_THREAD_COUNT);
        checkReceivedAllData(recvQueue, testData, LOAD_TEST_ITERATIONS);
        checkInputQueueIsEmpty();
        checkTotalNumConnections(server, 1);
    }

    private void checkTotalNumConnections(final TCPTestServer server, final int expectedTotalNumConnections) {
        assertEquals(expectedTotalNumConnections, server.getTotalNumConnections());
    }

    public abstract void configureProperties(final String host, final int port, final String outgoingMessageDelimiter, final boolean connectionPerFlowFile,
                                             final boolean expectValid) throws InitializationException;

    private void sendTestData(final String[] testData) {
        sendTestData(testData, DEFAULT_ITERATIONS, DEFAULT_THREAD_COUNT);
    }

    private void sendTestData(final String[] testData, final int iterations, final int threadCount) {
        runner.setThreadCount(threadCount);
        for (int i = 0; i < iterations; i++) {
            for (String item : testData) {
                runner.enqueue(item.getBytes());
            }
            runner.run(testData.length, false, i == 0 ? true : false);
        }
    }

    private void checkRelationships(final int successCount, final int failedCount) {
        runner.assertTransferCount(PutTCP.REL_SUCCESS, successCount);
        runner.assertTransferCount(PutTCP.REL_FAILURE, failedCount);
    }

    private void checkNoDataReceived(final ArrayBlockingQueue<List<Byte>> recvQueue) throws Exception {
        Thread.sleep(DATA_WAIT_PERIOD);
        assertNull(recvQueue.poll());
    }

    private void checkEmptyMessageReceived(final ArrayBlockingQueue<List<Byte>> recvQueue) throws Exception {
        Thread.sleep(DATA_WAIT_PERIOD);
        assertEquals(0, recvQueue.poll().size());
    }

    private void checkInputQueueIsEmpty() {
        runner.assertQueueEmpty();
    }

    private void checkReceivedAllData(final ArrayBlockingQueue<List<Byte>> recvQueue, final String[] sentData) throws Exception {
        checkReceivedAllData(recvQueue, sentData, DEFAULT_ITERATIONS);
    }

    private void checkReceivedAllData(final ArrayBlockingQueue<List<Byte>> recvQueue, final String[] sentData, final int iterations) throws Exception {
        // check each sent FlowFile was successfully sent and received.
        for (int i = 0; i < iterations; i++) {
            for (String item : sentData) {
                List<Byte> message = recvQueue.take();
                assertNotNull(message);
                Byte[] messageBytes = new Byte[message.size()];
                assertArrayEquals(item.getBytes(), ArrayUtils.toPrimitive(message.toArray(messageBytes)));
            }
        }

        runner.assertTransferCount(PutTCP.REL_SUCCESS, sentData.length * iterations);
        runner.clearTransferState();

        // Check that we have no unexpected extra data.
        assertNull(recvQueue.poll());
    }

    private String[] createContent(final int size) {
        final char[] content = new char[size];

        for (int i = 0; i < size; i++) {
            content[i] = CONTENT_CHAR;
        }

        return new String[] { new String(content) };
    }
}
