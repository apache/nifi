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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.processors.standard.util.TCPTestServer;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestPutTCP {
    private final static String TCP_SERVER_ADDRESS = "127.0.0.1";
    private final static String SERVER_VARIABLE = "server.address";
    private final static String TCP_SERVER_ADDRESS_EL = "${" + SERVER_VARIABLE + "}";
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
    private final static int DEFAULT_TEST_TIMEOUT_PERIOD = 10000;
    private final static int LONG_TEST_TIMEOUT_PERIOD = 300000;
    private final static String OUTGOING_MESSAGE_DELIMITER = "\n";
    private final static String OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR = "{delimiter}\r\n";
    private final static String[] EMPTY_FILE = { "" };
    private final static String[] VALID_FILES = { "abcdefghijklmnopqrstuvwxyz", "zyxwvutsrqponmlkjihgfedcba", "12345678", "343424222", "!@£$%^&*()_+:|{}[];\\" };

    private TCPTestServer server;
    private int port;
    private ArrayBlockingQueue<List<Byte>> received;
    private TestRunner runner;

    @Before
    public void setup() throws Exception {
        received = new ArrayBlockingQueue<>(BUFFER_SIZE);
        runner = TestRunners.newTestRunner(PutTCP.class);
        runner.setVariable(SERVER_VARIABLE, TCP_SERVER_ADDRESS);
    }

    @After
    public void cleanup() {
        runner.shutdown();
        shutdownServer();
    }

    @Test
    public void testPortProperty() {
        runner.setProperty(PutTCP.PORT, Integer.toString(MIN_INVALID_PORT));
        runner.assertNotValid();

        runner.setProperty(PutTCP.PORT, Integer.toString(MIN_VALID_PORT));
        runner.assertValid();

        runner.setProperty(PutTCP.PORT, Integer.toString(MAX_VALID_PORT));
        runner.assertValid();

        runner.setProperty(PutTCP.PORT, Integer.toString(MAX_INVALID_PORT));
        runner.assertNotValid();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testRunSuccess() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        assertServerConnections(1);
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessSslContextService() throws Exception {
        final TlsConfiguration tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();

        final SSLContext sslContext = SslContextUtils.createSslContext(tlsConfiguration);
        assertNotNull("SSLContext not found", sslContext);

        final String identifier = SSLContextService.class.getName();
        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(identifier);
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        runner.addControllerService(identifier, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(PutTCP.SSL_CONTEXT_SERVICE, identifier);

        final SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
        createTestServer(OUTGOING_MESSAGE_DELIMITER, false, serverSocketFactory);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        assertServerConnections(1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessServerVariableExpression() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS_EL, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        assertServerConnections(1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessPruneSenders() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertTransfers(VALID_FILES.length);
        assertMessagesReceived(VALID_FILES);

        runner.setProperty(PutTCP.IDLE_EXPIRATION, "500 ms");
        Thread.sleep(1000);
        runner.run(1, false, false);
        runner.clearTransferState();
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        assertServerConnections(2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessMultiCharDelimiter() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        assertServerConnections(1);
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessConnectionPerFlowFile() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER, true);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, true);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        assertServerConnections(VALID_FILES.length);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessConnectionFailure() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);

        shutdownServer();
        sendTestData(VALID_FILES);
        Thread.sleep(500);
        runner.assertQueueEmpty();

        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        assertServerConnections(1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessEmptyFile() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(EMPTY_FILE);
        assertTransfers(1);
        runner.assertQueueEmpty();
        assertServerConnections(1);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessLargeValidFile() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, true);
        final String[] testData = createContent(VALID_LARGE_FILE_SIZE);
        sendTestData(testData);
        assertMessagesReceived(testData);
        assertServerConnections(testData.length);
    }

    @Test(timeout = LONG_TEST_TIMEOUT_PERIOD)
    public void testRunSuccessFiveHundredMessages() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        Thread.sleep(1000);
        final String[] testData = createContent(VALID_SMALL_FILE_SIZE);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(testData, LOAD_TEST_ITERATIONS, LOAD_TEST_THREAD_COUNT);
        assertMessagesReceived(testData, LOAD_TEST_ITERATIONS);
        assertServerConnections(1);
    }

    private void createTestServer(final String delimiter) throws Exception {
        createTestServer(delimiter, false);
    }

    private void createTestServer(final String delimiter, final boolean closeOnMessageReceived) throws Exception {
        createTestServer(delimiter, closeOnMessageReceived, ServerSocketFactory.getDefault());
    }

    private void createTestServer(final String delimiter, final boolean closeOnMessageReceived, final ServerSocketFactory serverSocketFactory) throws Exception {
        server = new TCPTestServer(InetAddress.getByName(TCP_SERVER_ADDRESS), received, delimiter, closeOnMessageReceived);
        server.startServer(serverSocketFactory);
        port = server.getPort();
    }

    private void shutdownServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void configureProperties(String host, String outgoingMessageDelimiter, boolean connectionPerFlowFile) {
        runner.setProperty(PutTCP.HOSTNAME, host);
        runner.setProperty(PutTCP.PORT, Integer.toString(port));
        if (outgoingMessageDelimiter != null) {
            runner.setProperty(PutTCP.OUTGOING_MESSAGE_DELIMITER, outgoingMessageDelimiter);
        }

        runner.setProperty(PutTCP.CONNECTION_PER_FLOWFILE, String.valueOf(connectionPerFlowFile));
        runner.assertValid();
    }

    private void sendTestData(final String[] testData) {
        sendTestData(testData, DEFAULT_ITERATIONS, DEFAULT_THREAD_COUNT);
    }

    private void sendTestData(final String[] testData, final int iterations, final int threadCount) {
        runner.setThreadCount(threadCount);
        for (int i = 0; i < iterations; i++) {
            for (String item : testData) {
                runner.enqueue(item.getBytes());
            }
            runner.run(testData.length, false, i == 0);
        }
    }

    private void assertTransfers(final int successCount) {
        runner.assertTransferCount(PutTCP.REL_SUCCESS, successCount);
        runner.assertTransferCount(PutTCP.REL_FAILURE, 0);
    }

    private void assertMessagesReceived(final String[] sentData) throws Exception {
        assertMessagesReceived(sentData, DEFAULT_ITERATIONS);
        runner.assertQueueEmpty();
    }

    private void assertMessagesReceived(final String[] sentData, final int iterations) throws Exception {
        for (int i = 0; i < iterations; i++) {
            for (String item : sentData) {
                final List<Byte> message = received.take();
                assertNotNull(String.format("Message [%d] not found", i), message);
                final Byte[] messageBytes = new Byte[message.size()];
                assertArrayEquals(item.getBytes(), ArrayUtils.toPrimitive(message.toArray(messageBytes)));
            }
        }

        runner.assertTransferCount(PutTCP.REL_SUCCESS, sentData.length * iterations);
        runner.clearTransferState();

        assertNull("Unexpected Message Found", received.poll());
    }

    private void assertServerConnections(final int connections) {
        // Shutdown server to get completed number of connections
        shutdownServer();
        assertEquals("Server Connections not matched", server.getTotalNumConnections(), connections);
    }

    private String[] createContent(final int size) {
        final char[] content = new char[size];

        for (int i = 0; i < size; i++) {
            content[i] = CONTENT_CHAR;
        }

        return new String[] { new String(content) };
    }
}
