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
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TestListenTCP {
    private static final String SSL_CONTEXT_IDENTIFIER = SSLContextService.class.getName();

    private static final String LOCALHOST = "localhost";

    private static SSLContext keyStoreSslContext;

    private static SSLContext trustStoreSslContext;

    private TestRunner runner;

    @BeforeClass
    public static void configureServices() throws TlsException {
        keyStoreSslContext = SslContextUtils.createKeyStoreSslContext();
        trustStoreSslContext = SslContextUtils.createTrustStoreSslContext();
    }

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(ListenTCP.class);
    }

    @Test
    public void testCustomValidate() throws InitializationException {
        runner.setProperty(ListenTCP.PORT, "1");
        runner.assertValid();

        enableSslContextService(keyStoreSslContext);
        runner.setProperty(ListenTCP.CLIENT_AUTH, "");
        runner.assertNotValid();

        runner.setProperty(ListenTCP.CLIENT_AUTH, ClientAuth.REQUIRED.name());
        runner.assertValid();
    }

    @Test
    public void testRun() throws IOException {
        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        run(messages, messages.size(), null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    @Test
    public void testRunBatching() throws IOException {
        runner.setProperty(ListenTCP.MAX_BATCH_SIZE, "3");

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        run(messages, 2, null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);

        MockFlowFile mockFlowFile1 = mockFlowFiles.get(0);
        mockFlowFile1.assertContentEquals("This is message 1\nThis is message 2\nThis is message 3");

        MockFlowFile mockFlowFile2 = mockFlowFiles.get(1);
        mockFlowFile2.assertContentEquals("This is message 4\nThis is message 5");
    }

    @Test
    public void testRunClientAuthRequired() throws IOException, InitializationException {
        runner.setProperty(ListenTCP.CLIENT_AUTH, ClientAuth.REQUIRED.name());
        enableSslContextService(keyStoreSslContext);

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        run(messages, messages.size(), keyStoreSslContext);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    @Test
    public void testRunClientAuthNone() throws IOException, InitializationException {
        runner.setProperty(ListenTCP.CLIENT_AUTH, ClientAuth.NONE.name());
        enableSslContextService(keyStoreSslContext);

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        run(messages, messages.size(), trustStoreSslContext);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    protected void run(final List<String> messages, final int flowFiles, final SSLContext sslContext)
            throws IOException {

        final int port = NetworkUtils.availablePort();
        runner.setProperty(ListenTCP.PORT, Integer.toString(port));

        // Run Processor and start Dispatcher without shutting down
        runner.run(1, false, true);

        final String message = StringUtils.join(messages, null);
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        try (final Socket socket = getSocket(port, sslContext)) {
            final OutputStream outputStream = socket.getOutputStream();
            outputStream.write(bytes);
            outputStream.flush();

            // Run Processor for number of responses
            runner.run(flowFiles, false, false);

            runner.assertTransferCount(ListenTCP.REL_SUCCESS, flowFiles);
        } finally {
            runner.shutdown();
        }
    }

    private Socket getSocket(final int port, final SSLContext sslContext) throws IOException {
        final Socket socket;
        if (sslContext == null) {
            socket = new Socket(LOCALHOST, port);
        } else {
            socket = sslContext.getSocketFactory().createSocket(LOCALHOST, port);
        }
        return socket;
    }

    private void enableSslContextService(final SSLContext sslContext) throws InitializationException {
        final RestrictedSSLContextService sslContextService = Mockito.mock(RestrictedSSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_IDENTIFIER);
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        runner.addControllerService(SSL_CONTEXT_IDENTIFIER, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(ListenTCP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_IDENTIFIER);
    }
}
