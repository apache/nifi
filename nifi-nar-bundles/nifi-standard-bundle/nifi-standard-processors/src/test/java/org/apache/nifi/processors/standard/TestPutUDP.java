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

import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Timeout(10)
public class TestPutUDP {

    private final static String UDP_SERVER_ADDRESS = "127.0.0.1";
    private final static String SERVER_VARIABLE = "SERVER";
    private static final String DELIMITER = "\n";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final static int MAX_FRAME_LENGTH = 32800;
    private final static int VALID_LARGE_FILE_SIZE = 32768;
    private final static int INVALID_LARGE_FILE_SIZE = 1_000_000;
    private final static char CONTENT_CHAR = 'x';
    private final static int DATA_WAIT_PERIOD = 50;
    private final static String[] EMPTY_FILE = { "" };
    private final static String[] VALID_FILES = { "FIRST", "SECOND", "12345678", "343424222", "!@£$%^&*()_+:|{}[];\\" };

    private TestRunner runner;
    private int port;
    private EventServer eventServer;
    private BlockingQueue<ByteArrayMessage> messages;

    @BeforeEach
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(PutUDP.class);
        runner.setEnvironmentVariableValue(SERVER_VARIABLE, UDP_SERVER_ADDRESS);
        createTestServer(VALID_LARGE_FILE_SIZE);
    }

    @AfterEach
    public void cleanup() {
        runner.shutdown();
        removeTestServer();
    }

    @Test
    public void testSend() throws Exception {
        configureProperties();
        sendMessages(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        runner.assertQueueEmpty();
    }

    @Test
    public void testSendEmptyFile() throws Exception {
        configureProperties();
        sendMessages(EMPTY_FILE);
        checkRelationships(EMPTY_FILE.length, 0);
        checkNoDataReceived();
        runner.assertQueueEmpty();
    }

    @Test
    public void testSendLargeFile() throws Exception {
        configureProperties();
        final String[] testData = createContent(VALID_LARGE_FILE_SIZE);
        sendMessages(testData);
        assertMessagesReceived(testData);
        runner.assertQueueEmpty();
    }

    @Test
    public void testSendLargeFileInvalid() throws Exception {
        configureProperties();
        String[] testData = createContent(INVALID_LARGE_FILE_SIZE);
        sendMessages(testData);
        checkRelationships(0, testData.length);
        checkNoDataReceived();
        runner.assertQueueEmpty();
    }

    @Test
    public void testSendChangePropertiesAndSend() throws Exception {
        configureProperties();
        sendMessages(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        reset();

        configureProperties();
        sendMessages(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        reset();

        configureProperties();
        sendMessages(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
        runner.assertQueueEmpty();
    }

    private void reset() throws Exception {
        runner.clearTransferState();
        removeTestServer();
        createTestServer(MAX_FRAME_LENGTH);
    }

    private void configureProperties() {
        runner.setProperty(PutUDP.HOSTNAME, UDP_SERVER_ADDRESS);
        runner.setProperty(PutUDP.PORT, Integer.toString(port));
        runner.assertValid();
    }

    private void sendMessages(final String[] testData) {
        for (String item : testData) {
            runner.enqueue(item.getBytes());
            runner.run();
        }
    }

    private void checkRelationships(final int successCount, final int failedCount) {
        runner.assertTransferCount(PutUDP.REL_SUCCESS, successCount);
        runner.assertTransferCount(PutUDP.REL_FAILURE, failedCount);
    }

    private void checkNoDataReceived() throws Exception {
        Thread.sleep(DATA_WAIT_PERIOD);
        assertNull(messages.poll(), "Unexpected extra messages found");
    }

    private void assertMessagesReceived(final String[] sentMessages) throws Exception {
        // check each sent FlowFile was successfully sent and received.
         for (String item : sentMessages) {
             ByteArrayMessage packet = messages.take();
             assertNotNull(packet);
             assertArrayEquals(item.getBytes(), packet.getMessage());
        }

        runner.assertTransferCount(PutUDP.REL_SUCCESS, sentMessages.length);

        assertNull(messages.poll(), "Unexpected extra messages found");
    }

    private String[] createContent(final int size) {
        final char[] content = new char[size];

        for (int i = 0; i < size; i++) {
            content[i] = CONTENT_CHAR;
        }

        return new String[] { new String(content).concat("\n") };
    }

    private void createTestServer(final int frameSize) throws Exception {
        messages = new LinkedBlockingQueue<>();
        final byte[] delimiter = DELIMITER.getBytes(CHARSET);
        final InetAddress listenAddress = InetAddress.getByName(UDP_SERVER_ADDRESS);
        NettyEventServerFactory serverFactory = new ByteArrayMessageNettyEventServerFactory(
                runner.getLogger(), listenAddress, port, TransportProtocol.UDP, delimiter, frameSize, messages);
        serverFactory.setSocketReceiveBuffer(MAX_FRAME_LENGTH);
        serverFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        serverFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        eventServer = serverFactory.getEventServer();
        this.port = eventServer.getListeningPort();
    }

    private void removeTestServer() {
        if (eventServer != null) {
            eventServer.shutdown();
            eventServer = null;
        }
    }
}
