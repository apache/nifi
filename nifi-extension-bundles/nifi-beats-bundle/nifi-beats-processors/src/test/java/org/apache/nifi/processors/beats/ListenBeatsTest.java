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
package org.apache.nifi.processors.beats;

import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.processors.beats.protocol.FrameType;
import org.apache.nifi.processors.beats.protocol.ProtocolVersion;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;
import java.util.zip.DeflaterOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListenBeatsTest {

    private static final String LOCALHOST = "127.0.0.1";

    private static final String LOCALHOST_TRANSIT_URI = "beats://127.0.0.1:%d";

    private static final int ACK_PACKET_LENGTH = 6;

    private static final int FIRST_SEQUENCE_NUMBER = 1;

    private static final int INTEGER_BUFFER_SIZE = 4;

    private static final String JSON_PAYLOAD = "{\"@timestamp\":\"2022-10-31T12:30:45.678Z\",\"message\":\"Processing Started\"}";

    private static final int WINDOWED_MESSAGES = 50;

    TestRunner runner;

    @BeforeEach
    void createRunner() {
        runner = TestRunners.newTestRunner(ListenBeats.class);
    }

    @Timeout(10)
    @Test
    void testRunSingleJsonMessage() throws Exception {
        final int port = startServer();

        try (final Socket socket = new Socket(LOCALHOST, port);
             final InputStream inputStream = socket.getInputStream();
             final OutputStream outputStream = socket.getOutputStream()) {

            sendMessage(outputStream, FIRST_SEQUENCE_NUMBER);
            assertAckPacketMatched(inputStream, FIRST_SEQUENCE_NUMBER);
        }

        assertFlowFilesSuccess(1);
        assertReceiveEventFound(port);
    }

    @Timeout(10)
    @Test
    void testRunWindowSizeJsonMessages() throws Exception {
        final int port = startServer();

        try (final Socket socket = new Socket(LOCALHOST, port);
             final InputStream inputStream = socket.getInputStream();
             final OutputStream outputStream = socket.getOutputStream()) {

            sendWindowSize(outputStream);

            for (int sequenceNumber = FIRST_SEQUENCE_NUMBER; sequenceNumber <= WINDOWED_MESSAGES; sequenceNumber++) {
                sendMessage(outputStream, sequenceNumber);
            }

            assertAckPacketMatched(inputStream, WINDOWED_MESSAGES);
        }

        assertFlowFilesSuccess(WINDOWED_MESSAGES);
        assertReceiveEventFound(port);
    }

    @Timeout(10)
    @Test
    void testRunWindowSizeCompressedJsonMessages() throws Exception {
        final int port = startServer();

        try (final Socket socket = new Socket(LOCALHOST, port);
             final InputStream inputStream = socket.getInputStream();
             final OutputStream outputStream = socket.getOutputStream()) {

            sendWindowSize(outputStream);

            final ByteArrayOutputStream compressedOutputStream = new ByteArrayOutputStream();
            final DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(compressedOutputStream);

            for (int sequenceNumber = FIRST_SEQUENCE_NUMBER; sequenceNumber <= WINDOWED_MESSAGES; sequenceNumber++) {
                sendMessage(deflaterOutputStream, sequenceNumber);
            }

            deflaterOutputStream.close();
            final byte[] compressed = compressedOutputStream.toByteArray();
            sendCompressed(outputStream, compressed);

            assertAckPacketMatched(inputStream, WINDOWED_MESSAGES);
        }

        assertFlowFilesSuccess(WINDOWED_MESSAGES);
        assertReceiveEventFound(port);
    }

    private int startServer() {
        runner.setProperty(ListenerProperties.PORT, "0");
        runner.run(1, false, true);

        final int port = ((ListenBeats) runner.getProcessor()).getListeningPort();

        return port;
    }

    private void assertReceiveEventFound(final int port) {
        final Optional<ProvenanceEventRecord> receiveRecord = runner.getProvenanceEvents().stream().filter(record ->
                ProvenanceEventType.RECEIVE == record.getEventType()
        ).findFirst();

        assertTrue(receiveRecord.isPresent());
        final ProvenanceEventRecord record = receiveRecord.get();

        final String expectedTransitUri = String.format(LOCALHOST_TRANSIT_URI, port);
        assertEquals(expectedTransitUri, record.getTransitUri());
    }

    private void assertFlowFilesSuccess(final int expectedFlowFiles) {
        runner.run(expectedFlowFiles, true, false);
        runner.assertTransferCount(ListenBeats.REL_SUCCESS, expectedFlowFiles);

        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListenBeats.REL_SUCCESS).iterator();
        int i = 1;
        while (flowFiles.hasNext()) {
            final MockFlowFile flowFile = flowFiles.next();
            final String content = flowFile.getContent();
            assertEquals(JSON_PAYLOAD, content, String.format("FlowFile Content [%d] not matched", i));
            i++;
        }
    }

    private void sendWindowSize(final OutputStream outputStream) throws IOException {
        outputStream.write(ProtocolVersion.VERSION_2.getCode());

        outputStream.write(FrameType.WINDOW_SIZE.getCode());

        final byte[] windowSize = getUnsignedInteger(WINDOWED_MESSAGES);
        outputStream.write(windowSize);

        outputStream.flush();
    }

    private void sendMessage(final OutputStream outputStream, final int sequenceNumber) throws IOException {
        outputStream.write(ProtocolVersion.VERSION_2.getCode());

        outputStream.write(FrameType.JSON.getCode());

        final byte[] sequenceNumberEncoded = getUnsignedInteger(sequenceNumber);
        outputStream.write(sequenceNumberEncoded);

        final int payloadLength = JSON_PAYLOAD.length();

        final byte[] payloadSize = getUnsignedInteger(payloadLength);
        outputStream.write(payloadSize);

        outputStream.write(JSON_PAYLOAD.getBytes(StandardCharsets.UTF_8));

        outputStream.flush();
    }

    private void sendCompressed(final OutputStream outputStream, final byte[] compressed) throws IOException {
        outputStream.write(ProtocolVersion.VERSION_2.getCode());

        outputStream.write(FrameType.COMPRESSED.getCode());

        final int payloadLength = compressed.length;

        final byte[] payloadSize = getUnsignedInteger(payloadLength);
        outputStream.write(payloadSize);

        outputStream.write(compressed);

        outputStream.flush();
    }

    private void assertAckPacketMatched(final InputStream inputStream, final int expectedSequenceNumber) throws IOException {
        final byte[] ackPacket = new byte[ACK_PACKET_LENGTH];
        final int bytesRead = inputStream.read(ackPacket);

        assertEquals(ACK_PACKET_LENGTH, bytesRead);

        final ByteBuffer ackPacketBuffer = ByteBuffer.wrap(ackPacket);

        final byte version = ackPacketBuffer.get();
        assertEquals(ProtocolVersion.VERSION_2.getCode(), version);

        final byte frameType = ackPacketBuffer.get();
        assertEquals(FrameType.ACK.getCode(), frameType);

        final int sequenceNumber = ackPacketBuffer.getInt();
        assertEquals(expectedSequenceNumber, sequenceNumber);
    }

    private byte[] getUnsignedInteger(final int number) {
        return ByteBuffer.allocate(INTEGER_BUFFER_SIZE).putInt(number).array();
    }
}
