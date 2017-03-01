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

package org.apache.nifi.processors.gettcp;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public final class TestGetTCP {
    private TestRunner testRunner;
    private GetTCP processor;

    @Before
    public void setup() {
        processor = new GetTCP();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testSelectPropertiesValidation() {
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "!@;;*blah:9999");
        testRunner.assertNotValid();
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
        testRunner.setProperty(GetTCP.END_OF_MESSAGE_BYTE, "354");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.END_OF_MESSAGE_BYTE, "13");
        testRunner.assertValid();
    }

    @Test
    public void testDynamicProperty() {
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999,localhost:1234");
        testRunner.setProperty("MyCustomProperty", "abc");
        testRunner.assertValid();
    }

    @Ignore("test is brittle as depends on timing - not reliable across systems, in parallel builds, etc..")
    @Test
    public void testSuccessInteraction() throws Exception {
        int port = this.availablePort();
        Server server = setupTCPServer(port);
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:" + port);
        testRunner.run(1000, false);
        this.sendToSocket(new InetSocketAddress(port), "Hello\r");
        Thread.sleep(200);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 1);
        testRunner.clearTransferState();
        testRunner.shutdown();
        server.stop();
    }

    @Ignore("test is brittle as depends on timing - not reliable across systems, in parallel builds, etc..")
    @Test
    public void testPartialInteraction() throws Exception {
        int port = this.availablePort();
        Server server = setupTCPServer(port);
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:" + port);
        testRunner.setProperty(GetTCP.RECEIVE_BUFFER_SIZE, "2B");
        testRunner.run(1000, false);
        this.sendToSocket(new InetSocketAddress(port), "Hello\r");
        Thread.sleep(300);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_PARTIAL, 3);
        testRunner.clearTransferState();

        this.sendToSocket(new InetSocketAddress(port), "H\r");
        Thread.sleep(300);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 1);
        testRunner.clearTransferState();
        testRunner.shutdown();
        server.stop();
    }

    private Server setupTCPServer(int port) {
        InetSocketAddress address = new InetSocketAddress(port);
        Server server = new Server(address, 1024, (byte) '\r');
        server.start();
        return server;
    }

    private void sendToSocket(InetSocketAddress address, String message) throws Exception {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.write(message);
        out.flush();
        socket.close();
    }

    /**
     * Will determine the available port used by test server.
     */
    private int availablePort() {
        ServerSocket s = null;
        try {
            s = new ServerSocket(0);
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to discover available port.", e);
        } finally {
            try {
                s.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
