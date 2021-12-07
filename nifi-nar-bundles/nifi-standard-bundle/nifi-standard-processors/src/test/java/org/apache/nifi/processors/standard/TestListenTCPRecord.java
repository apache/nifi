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

import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.ByteArrayNettyEventSenderFactory;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class TestListenTCPRecord {
    static final String SCHEMA_TEXT = "{\n" +
            "  \"name\": \"syslogRecord\",\n" +
            "  \"namespace\": \"nifi\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    { \"name\": \"timestamp\", \"type\": \"string\" },\n" +
            "    { \"name\": \"logsource\", \"type\": \"string\" },\n" +
            "    { \"name\": \"message\", \"type\": \"string\" }\n" +
            "  ]\n" +
            "}";

    static final String THREE_MESSAGES = "[" +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 1\"}," +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 2\"}," +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 3\"}" +
            "]";

    private static final Logger LOGGER = LoggerFactory.getLogger(TestListenTCPRecord.class);
    private static final Duration SENDER_TIMEOUT = Duration.ofSeconds(10);

    private static final long TEST_TIMEOUT = 900000;

    private static final String LOCALHOST = "localhost";

    private static final String SSL_CONTEXT_IDENTIFIER = SSLContextService.class.getName();

    private static SSLContext keyStoreSslContext;

    private static SSLContext trustStoreSslContext;

    private TestRunner runner;

    @BeforeClass
    public static void configureServices() throws TlsException {
        keyStoreSslContext = SslContextUtils.createKeyStoreSslContext();
        trustStoreSslContext = SslContextUtils.createTrustStoreSslContext();
    }

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(ListenTCPRecord.class);

        final String readerId = "record-reader";
        final RecordReaderFactory readerFactory = new JsonTreeReader();
        runner.addControllerService(readerId, readerFactory);
        runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_TEXT, SCHEMA_TEXT);
        runner.enableControllerService(readerFactory);

        final String writerId = "record-writer";
        final RecordSetWriterFactory writerFactory = new MockRecordWriter("timestamp, logsource, message");
        runner.addControllerService(writerId, writerFactory);
        runner.enableControllerService(writerFactory);

        runner.setProperty(ListenTCPRecord.RECORD_READER, readerId);
        runner.setProperty(ListenTCPRecord.RECORD_WRITER, writerId);
    }

    @Test
    public void testCustomValidate() throws InitializationException {
        runner.setProperty(ListenTCPRecord.PORT, "1");
        runner.assertValid();

        enableSslContextService(keyStoreSslContext);
        runner.setProperty(ListenTCPRecord.CLIENT_AUTH, "");
        runner.assertNotValid();

        runner.setProperty(ListenTCPRecord.CLIENT_AUTH, ClientAuth.REQUIRED.name());
        runner.assertValid();
    }

    @Test
    public void testRunOneRecordPerFlowFile() throws Exception {
        runner.setProperty(ListenTCPRecord.RECORD_BATCH_SIZE, "1");

        run(3, THREE_MESSAGES.getBytes(StandardCharsets.UTF_8), null, true);

        Assert.assertEquals(3 , runner.getFlowFilesForRelationship(ListenTCPRecord.REL_SUCCESS).size());
        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCPRecord.REL_SUCCESS);
        for (int i = 0; i < mockFlowFiles.size(); i++) {
            final MockFlowFile flowFile = mockFlowFiles.get(i);
            flowFile.assertAttributeEquals("record.count", "1");

            final String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("This is a test " + (i + 1)));
        }
    }

    @Test
    public void testRunMultipleRecordsPerFlowFileLessThanBatchSize() throws Exception {
        runner.setProperty(ListenTCPRecord.RECORD_BATCH_SIZE, "5");

        //runner.run(1, false, true);
        runner.getLogger().info("Sending 3 messages");
        run(1, THREE_MESSAGES.getBytes(StandardCharsets.UTF_8), null, true);
        runner.getLogger().info("Sending another 3 messages");
        run(1, THREE_MESSAGES.getBytes(StandardCharsets.UTF_8), null, true);

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCPRecord.REL_SUCCESS);
        Assert.assertEquals(1, mockFlowFiles.size());

        final MockFlowFile flowFile = mockFlowFiles.get(0);
        flowFile.assertAttributeEquals("record.count", "3");

        final String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertNotNull(content);
        Assert.assertTrue(content.contains("This is a test " + 1));
        Assert.assertTrue(content.contains("This is a test " + 2));
        Assert.assertTrue(content.contains("This is a test " + 3));
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testRunClientAuthRequired() throws Exception {
        runner.setProperty(ListenTCPRecord.CLIENT_AUTH, ClientAuth.REQUIRED.name());
        enableSslContextService(keyStoreSslContext);

        run(1, THREE_MESSAGES.getBytes(StandardCharsets.UTF_8), keyStoreSslContext, true);

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCPRecord.REL_SUCCESS);
        Assert.assertEquals(1, mockFlowFiles.size());

        final String content = new String(mockFlowFiles.get(0).toByteArray(), StandardCharsets.UTF_8);
        Assert.assertNotNull(content);
        Assert.assertTrue(content.contains("This is a test " + 1));
        Assert.assertTrue(content.contains("This is a test " + 2));
        Assert.assertTrue(content.contains("This is a test " + 3));
    }

    @Test
    public void testRunClientAuthNone() throws Exception {
        runner.setProperty(ListenTCPRecord.CLIENT_AUTH, ClientAuth.NONE.name());
        enableSslContextService(keyStoreSslContext);

        run(1, THREE_MESSAGES.getBytes(StandardCharsets.UTF_8), trustStoreSslContext, true);

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCPRecord.REL_SUCCESS);
        Assert.assertEquals(1, mockFlowFiles.size());

        final String content = new String(mockFlowFiles.get(0).toByteArray(), StandardCharsets.UTF_8);
        Assert.assertNotNull(content);
        Assert.assertTrue(content.contains("This is a test " + 1));
        Assert.assertTrue(content.contains("This is a test " + 2));
        Assert.assertTrue(content.contains("This is a test " + 3));
    }

    protected void run(final int expectedTransferred, final byte[] data, final SSLContext sslContext, final boolean shouldInitialize) throws Exception {
        final int port = NetworkUtils.availablePort();
        runner.setProperty(ListenTCPRecord.PORT, Integer.toString(port));

        // Run Processor and start listener without shutting down
        runner.run(1, false, shouldInitialize);
        sendMessages(port, data, sslContext);

        // Run Processor until success leveraging test method timeouts for failure status
        int iterations = 0;
        while (getSuccessCount() < expectedTransferred) {
            runner.run(1, false, false);
            iterations++;

            final Optional<LogMessage> firstErrorMessage = runner.getLogger().getErrorMessages().stream().findFirst();
            Assert.assertNull(firstErrorMessage.orElse(null));
        }
        LOGGER.info("Completed after iterations [{}]", iterations);
    }

    private int getSuccessCount() {
        return runner.getFlowFilesForRelationship(ListenTCPRecord.REL_SUCCESS).size();
    }

    private void enableSslContextService(final SSLContext sslContext) throws InitializationException {
        final RestrictedSSLContextService sslContextService = Mockito.mock(RestrictedSSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_IDENTIFIER);
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        runner.addControllerService(SSL_CONTEXT_IDENTIFIER, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(ListenTCPRecord.SSL_CONTEXT_SERVICE, SSL_CONTEXT_IDENTIFIER);
    }

    private void sendMessages(final int port, final byte[] data, final SSLContext sslContext) throws Exception {
        final ByteArrayNettyEventSenderFactory eventSenderFactory = new ByteArrayNettyEventSenderFactory(runner.getLogger(), LOCALHOST, port, TransportProtocol.TCP);
        eventSenderFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        eventSenderFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        if (sslContext != null) {
            eventSenderFactory.setSslContext(sslContext);
        }

        eventSenderFactory.setTimeout(SENDER_TIMEOUT);
        try (final EventSender<byte[]> eventSender = eventSenderFactory.getEventSender()) {
            eventSender.sendEvent(data);
        }
    }

}
