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
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.standard.property.TransmissionStrategy;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Timeout(30)
@ExtendWith(MockitoExtension.class)
public class TestPutTCP {
    private final static String TCP_SERVER_ADDRESS = "127.0.0.1";
    private final static String SERVER_VARIABLE = "server.address";
    private final static String TCP_SERVER_ADDRESS_EL = "${" + SERVER_VARIABLE + "}";
    private final static int VALID_LARGE_FILE_SIZE = 32768;
    private final static int VALID_SMALL_FILE_SIZE = 64;
    private final static int LOAD_TEST_ITERATIONS = 500;
    private final static int LOAD_TEST_THREAD_COUNT = 1;
    private final static int DEFAULT_ITERATIONS = 1;
    private final static int DEFAULT_THREAD_COUNT = 1;
    private final static char CONTENT_CHAR = 'x';
    private final static String OUTGOING_MESSAGE_DELIMITER = "\n";
    private final static String OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR = "{delimiter}\r\n";
    private final static String[] EMPTY_FILE = {""};
    private final static String[] VALID_FILES = {"abcdefghijklmnopqrstuvwxyz", "zyxwvutsrqponmlkjihgfedcba", "12345678", "343424222", "!@£$%^&*()_+:|{}[];\\"};

    private static final String WRITER_SERVICE_ID = RecordSetWriterFactory.class.getSimpleName();

    private static final String READER_SERVICE_ID = RecordReaderFactory.class.getSimpleName();

    private static final String RECORD = String.class.getSimpleName();

    private static final Record NULL_RECORD = null;

    @Mock
    private RecordSetWriterFactory writerFactory;

    @Mock
    private RecordReaderFactory readerFactory;

    @Mock
    private RecordReader recordReader;

    @Mock
    private Record record;

    private EventServer eventServer;
    private TestRunner runner;
    private BlockingQueue<ByteArrayMessage> messages;

    @BeforeEach
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(PutTCP.class);
        runner.setEnvironmentVariableValue(SERVER_VARIABLE, TCP_SERVER_ADDRESS);
    }

    @AfterEach
    public void cleanup() {
        runner.shutdown();
        shutdownServer();
    }

    @Test
    public void testRunSuccess() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
    }

    @Test
    public void testRunSuccessSslContextService() throws Exception {
        final SSLContext sslContext = getSslContext();
        final String identifier = SSLContextProvider.class.getName();
        final SSLContextProvider sslContextProvider = Mockito.mock(SSLContextProvider.class);
        Mockito.when(sslContextProvider.getIdentifier()).thenReturn(identifier);
        Mockito.when(sslContextProvider.createContext()).thenReturn(sslContext);
        runner.addControllerService(identifier, sslContextProvider);
        runner.enableControllerService(sslContextProvider);
        runner.setProperty(PutTCP.SSL_CONTEXT_SERVICE, identifier);
        createTestServer(sslContext, OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
    }

    @Test
    public void testRunSuccessServerVariableExpression() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS_EL, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
    }

    @Test
    public void testRunSuccessPruneSenders() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertTransfers(VALID_FILES.length);
        assertMessagesReceived(VALID_FILES);

        runner.setProperty(PutTCP.IDLE_EXPIRATION, "500 ms");
        runner.run(1, false, false);
        runner.clearTransferState();
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
    }

    @Test
    public void testRunSuccessMultiCharDelimiter() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER_MULTI_CHAR, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
    }

    @Test
    public void testRunSuccessConnectionPerFlowFile() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, true);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
    }

    @Test
    public void testRunSuccessConnectionFailure() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);

        shutdownServer();
        sendTestData(VALID_FILES);
        runner.assertQueueEmpty();

        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(VALID_FILES);
        assertMessagesReceived(VALID_FILES);
    }

    @Test
    public void testRunSuccessEmptyFile() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        sendTestData(EMPTY_FILE);
        assertTransfers(1);
        runner.assertQueueEmpty();
    }

    @Test
    public void testRunSuccessLargeValidFile() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, true);
        final String[] testData = createContent(VALID_LARGE_FILE_SIZE);
        sendTestData(testData);
        assertMessagesReceived(testData);
    }

    @Test
    public void testRunSuccessFiveHundredMessages() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        configureProperties(TCP_SERVER_ADDRESS, OUTGOING_MESSAGE_DELIMITER, false);
        final String[] testData = createContent(VALID_SMALL_FILE_SIZE);
        sendTestData(testData, LOAD_TEST_ITERATIONS, LOAD_TEST_THREAD_COUNT);
        assertMessagesReceived(testData, LOAD_TEST_ITERATIONS);
    }

    @Test
    void testRunSuccessRecordOriented() throws Exception {
        createTestServer(OUTGOING_MESSAGE_DELIMITER);
        runner.setProperty(PutTCP.HOSTNAME, TCP_SERVER_ADDRESS);
        runner.setProperty(PutTCP.PORT, String.valueOf(eventServer.getListeningPort()));

        runner.setProperty(PutTCP.TRANSMISSION_STRATEGY, TransmissionStrategy.RECORD_ORIENTED);

        when(writerFactory.getIdentifier()).thenReturn(WRITER_SERVICE_ID);
        runner.addControllerService(WRITER_SERVICE_ID, writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty(PutTCP.RECORD_WRITER, WRITER_SERVICE_ID);

        when(readerFactory.getIdentifier()).thenReturn(READER_SERVICE_ID);
        runner.addControllerService(READER_SERVICE_ID, readerFactory);
        runner.enableControllerService(readerFactory);
        runner.setProperty(PutTCP.RECORD_READER, READER_SERVICE_ID);

        when(readerFactory.createRecordReader(any(), any(), any())).thenReturn(recordReader);
        when(recordReader.nextRecord()).thenReturn(record, NULL_RECORD);
        when(writerFactory.createWriter(any(), any(), any(OutputStream.class), any(FlowFile.class))).thenAnswer((Answer<RecordSetWriter>) invocationOnMock -> {
            final OutputStream outputStream = invocationOnMock.getArgument(2, OutputStream.class);
            return new TestPutTCP.MockRecordSetWriter(outputStream);
        });

        runner.enqueue(RECORD);
        runner.run();

        runner.assertTransferCount(PutTCP.REL_FAILURE, 0);

        final Iterator<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(PutTCP.REL_SUCCESS).iterator();
        assertTrue(successFlowFiles.hasNext(), "Success FlowFiles not found");
        final MockFlowFile successFlowFile = successFlowFiles.next();
        successFlowFile.assertAttributeEquals(PutTCP.RECORD_COUNT_TRANSMITTED, Integer.toString(1));

        final List<ProvenanceEventRecord> provenanceEventRecords = runner.getProvenanceEvents();
        final Optional<ProvenanceEventRecord> sendEventFound = provenanceEventRecords.stream()
                .filter(eventRecord -> ProvenanceEventType.SEND == eventRecord.getEventType())
                .findFirst();
        assertTrue(sendEventFound.isPresent(), "Provenance Send Event not found");
        final ProvenanceEventRecord sendEventRecord = sendEventFound.get();
        assertTrue(sendEventRecord.getTransitUri().contains(TCP_SERVER_ADDRESS), "Transit URI not matched");

        final ByteArrayMessage message = messages.take();
        assertNotNull(message);
        assertArrayEquals(RECORD.getBytes(StandardCharsets.UTF_8), message.getMessage());
        assertEquals(TCP_SERVER_ADDRESS, message.getSender());
    }

    private void createTestServer(final String delimiter) throws UnknownHostException {
        createTestServer(null, delimiter);
    }

    private void createTestServer(final SSLContext sslContext, final String delimiter) throws UnknownHostException {
        messages = new LinkedBlockingQueue<>();
        final InetAddress listenAddress = InetAddress.getByName(TCP_SERVER_ADDRESS);
        NettyEventServerFactory serverFactory = new ByteArrayMessageNettyEventServerFactory(runner.getLogger(),
                listenAddress, 0, TransportProtocol.TCP, delimiter.getBytes(), VALID_LARGE_FILE_SIZE, messages);
        if (sslContext != null) {
            serverFactory.setSslContext(sslContext);
        }
        serverFactory.setShutdownQuietPeriod(Duration.ZERO);
        serverFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        eventServer = serverFactory.getEventServer();
    }

    private void shutdownServer() {
        if (eventServer != null) {
            eventServer.shutdown();
        }
    }

    private void configureProperties(final String host, final String outgoingMessageDelimiter, final boolean connectionPerFlowFile) {
        runner.setProperty(PutTCP.HOSTNAME, host);
        runner.setProperty(PutTCP.PORT, String.valueOf(eventServer.getListeningPort()));

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
            for (String ignored : sentData) {
                final ByteArrayMessage message = messages.take();
                assertNotNull(message, String.format("Message [%d] not found", i));
                assertTrue(Arrays.asList(sentData).contains(new String(message.getMessage())));
            }
        }

        runner.assertTransferCount(PutTCP.REL_SUCCESS, sentData.length * iterations);
        runner.clearTransferState();

        assertNull(messages.poll(), "Unexpected extra messages found");
    }

    private String[] createContent(final int size) {
        final char[] content = new char[size];
        Arrays.fill(content, CONTENT_CHAR);
        return new String[] {new String(content)};
    }

    private SSLContext getSslContext() throws GeneralSecurityException {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        return new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(new char[]{})
                .build();
    }

    private static class MockRecordSetWriter implements RecordSetWriter {
        private final OutputStream outputStream;

        private MockRecordSetWriter(final OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public WriteResult write(final RecordSet recordSet) {
            return WriteResult.EMPTY;
        }

        @Override
        public void beginRecordSet() {

        }

        @Override
        public WriteResult finishRecordSet() {
            return WriteResult.EMPTY;
        }

        @Override
        public WriteResult write(Record record) throws IOException {
            outputStream.write(RECORD.getBytes(StandardCharsets.UTF_8));
            outputStream.write(OUTGOING_MESSAGE_DELIMITER.getBytes(StandardCharsets.UTF_8));
            return WriteResult.of(1, Collections.emptyMap());
        }

        @Override
        public String getMimeType() {
            return null;
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }
    }
}
