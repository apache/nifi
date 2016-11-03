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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public final class TestGetTCP {
    private ServerSocketChannel serverSocketChannel;
    private TestRunner testRunner;
    private MockGetTCP processor;
    private int listeningPort = 9999;
    private SocketChannel clientSocket;

    @Before
    public void setup() {
        processor = new MockGetTCP();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testCustomPropertyValidator() {
        //this only tests the properties that use the custom validator. It is assumed here
        //that the rest of the NiFi test cases cover the built in validators.
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999");
        testRunner.assertValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:-1");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, ",");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, ",localhost:9999");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "999,localhost:123");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:abc_port");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999;localhost:1234");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999,localhost:1234");
        testRunner.assertValid();
        testRunner.setProperty(GetTCP.FAILOVER_ENDPOINT, "127.0.0.1;1234");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.FAILOVER_ENDPOINT, "127.0.0.1:1234");
        testRunner.assertValid();

    }

    @Test
    public void testDynamicProperty() {
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999,localhost:1234");
        testRunner.setProperty("MyCustomProperty", "abc");
        testRunner.assertValid();
    }

    @Test
    public void testConnection() {
        setupTCPServer();
        setUpStandardTestConfig();
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 1);
        testRunner.clearTransferState();
        shutdownTCPServer();
    }


    @Test
    public void testFailOver() {
        setupTCPServer(9999);
        setUpStandardTestConfig("10",9998,9999);
        processor.backupServer = "localhost:9999";
        processor.mainPort = 9998;
        testRunner.run(1, true, true);

        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 1);
        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(GetTCP.REL_SUCCESS);
        assertNotNull(files);
        assertTrue(1 == files.size());
        testRunner.clearTransferState();
        shutdownTCPServer();
    }

    @Test(expected = AssertionError.class)
    public void testFailAllHosts() {
        setupTCPServer(9999);
        setUpStandardTestConfig("10",9998,9997);
        processor.backupServer = "localhost:9997";
        processor.mainPort = 9998;
        testRunner.run(1, true, true);
        shutdownTCPServer();
    }

    @Test
    public void testReceiveSingleMessages() throws IOException {
        setupTCPServer();
        setUpStandardTestConfig();
        processor.numMessagesToSend = 10;
        testRunner.run(10, true, true);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 10);
        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(GetTCP.REL_SUCCESS);
        assertNotNull(files);
        assertTrue(10 == files.size());
        testRunner.clearTransferState();
        shutdownTCPServer();
    }

    @Test
    public void testBatchMessages() throws IOException {
        setupTCPServer();
        setUpStandardTestConfig("10");
        processor.numMessagesToSend = 10;
        testRunner.run(1, true, true);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 1);
        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(GetTCP.REL_SUCCESS);
        assertNotNull(files);
        assertTrue(1 == files.size());
        testRunner.clearTransferState();
        shutdownTCPServer();
    }

    private void setUpStandardTestConfig() {
        setUpStandardTestConfig("1",listeningPort,listeningPort +1);
    }

    private void setUpStandardTestConfig(final String batchSize) {
        setUpStandardTestConfig(batchSize,listeningPort,listeningPort +1);
    }
    private void setUpStandardTestConfig(final String bacthSize,int endpointPort, int backupPort) {
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:" + endpointPort);
        testRunner.setProperty(GetTCP.BATCH_SIZE, bacthSize);
        testRunner.setProperty(GetTCP.CONNECTION_ATTEMPT_COUNT, "1");
        testRunner.setProperty(GetTCP.FAILOVER_ENDPOINT, "localhost:" + backupPort);
        testRunner.assertValid();
    }
    private void setupTCPServer(int port) {
        try {
            serverSocketChannel = ServerSocketChannel.open();

            InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", port);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(inetSocketAddress);
            serverSocketChannel.accept();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    private void setupTCPServer() {
        setupTCPServer(listeningPort);
    }
    private void shutdownTCPServer() {
        try {
            if(null != serverSocketChannel) {
                serverSocketChannel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected class MockGetTCP extends GetTCP {

        int numMessagesToSend;
        int mainPort;
        String backupServer;
        boolean killPrimary;


        @Override
        protected SocketRecveiverThread createSocketRecveiverThread(int rcvBufferSize, boolean keepAlive, int connectionTimeout, int connectionRetryCount, String[] hostAndPort) {
            return new MockSocketReceiverThread(hostAndPort[0],
                    Integer.parseInt(hostAndPort[1]), backupServer, keepAlive, rcvBufferSize, connectionTimeout, connectionRetryCount);
        }

        class MockSocketReceiverThread extends GetTCP.SocketRecveiverThread {

            final String backup;
            MockSocketReceiverThread(final String host, final int port, final String backupServer, final boolean keepAlive,
                                     final int rcvBufferSize,
                                     final int connectionTimeout,
                                     final int maxConnectionAttemptCount) {

                super(host, port, backupServer, keepAlive, rcvBufferSize, connectionTimeout, maxConnectionAttemptCount);
                this.backup = backupServer;
            }

            @Override
            public void run() {
                for (int i = 0; i < numMessagesToSend; i++) {
                    socketMessagesReceived.offer("Message number: " + i);
                }

            }

        }
    }
}
