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
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.ByteArrayNettyEventSenderFactory;
import org.apache.nifi.event.transport.netty.ParsingStrategy;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestListenTCP {
    private static final String CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE = "client.certificate.subject.dn";
    private static final String CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE = "client.certificate.issuer.dn";
    private static final String SSL_CONTEXT_IDENTIFIER = SSLContextService.class.getName();

    private static final String LOCALHOST = "localhost";
    private static final Duration SENDER_TIMEOUT = Duration.ofSeconds(10);

    private static SSLContext keyStoreSslContext;

    private static SSLContext trustStoreSslContext;

    private TestRunner runner;

    @BeforeAll
    public static void configureServices() throws TlsException {
        keyStoreSslContext = SslContextUtils.createKeyStoreSslContext();
        trustStoreSslContext = SslContextUtils.createTrustStoreSslContext();
    }

    @BeforeEach
    public void setup() {
        runner = TestRunners.newTestRunner(ListenTCP.class);
    }

    @Test
    public void testCustomValidate() throws Exception {
        runner.setProperty(ListenerProperties.PORT, "1");
        runner.assertValid();

        enableSslContextService(keyStoreSslContext);
        runner.setProperty(ListenTCP.CLIENT_AUTH, "");
        runner.assertNotValid();

        runner.setProperty(ListenTCP.CLIENT_AUTH, ClientAuth.REQUIRED.name());
        runner.assertValid();
    }

    @Test
    public void testRun() throws Exception {
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
    public void testRunBatching() throws Exception {
        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "3");
        runner.setProperty(ListenTCP.POOL_RECV_BUFFERS, "False");

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
    public void testRunClientAuthRequired() throws Exception {
        final String expectedDistinguishedName = "CN=localhost";
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
            mockFlowFiles.get(i).assertAttributeExists(CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE);
            mockFlowFiles.get(i).assertAttributeExists(CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE);
            mockFlowFiles.get(i).assertAttributeEquals(CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE, expectedDistinguishedName);
            mockFlowFiles.get(i).assertAttributeEquals(CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE, expectedDistinguishedName);
        }
    }

    @Test
    public void testRunClientAuthNone() throws Exception {
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
        assertEquals(5, mockFlowFiles.size());
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
            mockFlowFiles.get(i).assertAttributeNotExists(CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE);
            mockFlowFiles.get(i).assertAttributeNotExists(CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE);
        }
    }

    @Test
    public void testRunParsingDisabled() throws Exception {
        runner.setProperty(ListenTCP.PARSING_STRATEGY, "" + ParsingStrategy.DISABLED);

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1");
        messages.add("This is message 2");
        messages.add("This is message 3");
        messages.add("5 This is message 4");
        messages.add("This is\nmessage 5");

        runner.setProperty(ListenerProperties.PORT, "0");
        runner.run(1, false, true);
        final int port = ((ListenTCP) runner.getProcessor()).getListeningPort();
        for (String message: messages) {
            final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
            sendMessages(port, bytes, null);
        }
        runner.run(messages.size(), false, false);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        assertEquals(5, mockFlowFiles.size());
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals(messages.get(i));
        }
    }

    @Test
    public void testRunOctetCountingSimpleStrict() throws Exception {
        runner.setProperty(ListenTCP.PARSING_STRATEGY, "" + ParsingStrategy.OCTET_COUNTING_STRICT);

        final List<String> messages = new ArrayList<>();
        messages.add("17 This is message 1");
        messages.add("17 This is message 2");
        messages.add("17 This is message 3");
        messages.add("17 This is message 4");
        messages.add("17 This is message 5");

        run(messages, messages.size(), null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        assertEquals(5, mockFlowFiles.size());
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    @Test
    public void testRunOctetCountingSimpleStrictFail() throws Exception {
        runner.setProperty(ListenTCP.PARSING_STRATEGY, "" + ParsingStrategy.OCTET_COUNTING_STRICT);

        final List<String> messages = new ArrayList<>();
        messages.add("17 This is message 1");
        messages.add("17 This is message 2");
        messages.add("fail This is message 3");

        run(messages, messages.size(), null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        assertEquals(2, mockFlowFiles.size());
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    @Test
    public void testRunOctetCountingComplex() throws Exception {
        runner.setProperty(ListenTCP.PARSING_STRATEGY, "" + ParsingStrategy.OCTET_COUNTING_TOLERANT);

        final List<String> messages = new ArrayList<>();
        messages.add("17 This is message 1");
        messages.add("17 This is message 2");
        messages.add("This is message 3\n");
        messages.add("17 This is message 4");
        messages.add("17 This is message 5");

        run(messages, messages.size(), null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        assertEquals(5, mockFlowFiles.size());
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    @Test
    public void testRunOctetCountingReplacement() throws Exception {
        runner.setProperty(ListenTCP.PARSING_STRATEGY, "" + ParsingStrategy.OCTET_COUNTING_TOLERANT);
        runner.setProperty(ListenTCP.DELIMITER_REPLACEMENT, "#012");

        final List<String> messages = new ArrayList<>();
        messages.add("17 This\nis message 1");
        messages.add("17 This\nis\nmessage 2");
        messages.add("18 This\nis message 3\n");

        run(messages, messages.size(), null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        mockFlowFiles.get(0).assertContentEquals("This#012is message 1");
        mockFlowFiles.get(1).assertContentEquals("This#012is#012message 2");
        mockFlowFiles.get(2).assertContentEquals("This#012is message 3\n");// delimiter at the end shouldn't be replaced

    }

    @Test
    public void testRunOctetCountingBatching() throws Exception {
        runner.setProperty(ListenerProperties.MAX_BATCH_SIZE, "3");
        runner.setProperty(ListenTCP.POOL_RECV_BUFFERS, "False");
        runner.setProperty(ListenTCP.PARSING_STRATEGY, "" + ParsingStrategy.OCTET_COUNTING_STRICT);

        final List<String> messages = new ArrayList<>();
        messages.add("18 This is message 1\n");
        messages.add("18 This is message 2\n");
        messages.add("18 This is message 3\n");
        messages.add("18 This is message 4\n");
        messages.add("18 This is message 5\n");

        run(messages, 10, null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);

        assertEquals(2, mockFlowFiles.size());

        // This test has a '\n' at the end. It differs from testRunBatching which doesn't have one, but the '\n' is
        // inside the OctetCounting frame, so it makes sense not to remove it.
        mockFlowFiles.get(0).assertContentEquals("This is message 1\nThis is message 2\nThis is message 3\n");

        mockFlowFiles.get(1).assertContentEquals("This is message 4\nThis is message 5\n");
    }

    private void run(final List<String> messages, final int flowFiles, final SSLContext sslContext) throws Exception {
        runner.setProperty(ListenerProperties.PORT, "0");
        final String message = StringUtils.join(messages, null);
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        runner.run(1, false, true);

        final int port = ((ListenTCP) runner.getProcessor()).getListeningPort();

        sendMessages(port, bytes, sslContext);
        runner.run(flowFiles, false, false);
    }

    private void enableSslContextService(final SSLContext sslContext) throws InitializationException {
        final RestrictedSSLContextService sslContextService = Mockito.mock(RestrictedSSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_IDENTIFIER);
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        runner.addControllerService(SSL_CONTEXT_IDENTIFIER, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(ListenTCP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_IDENTIFIER);
    }

    private void sendMessages(final int port, final byte[] messages, final SSLContext sslContext) throws Exception {
        final ByteArrayNettyEventSenderFactory eventSenderFactory = new ByteArrayNettyEventSenderFactory(runner.getLogger(), LOCALHOST, port, TransportProtocol.TCP);
        eventSenderFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        eventSenderFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        if (sslContext != null) {
            eventSenderFactory.setSslContext(sslContext);
        }

        eventSenderFactory.setTimeout(SENDER_TIMEOUT);
        try (final EventSender<byte[]> eventSender = eventSenderFactory.getEventSender()) {
            eventSender.sendEvent(messages);
        }
    }
}
