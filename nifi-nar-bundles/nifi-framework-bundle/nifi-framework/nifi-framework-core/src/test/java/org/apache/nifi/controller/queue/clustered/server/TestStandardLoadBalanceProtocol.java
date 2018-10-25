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

package org.apache.nifi.controller.queue.clustered.server;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CHECK_SPACE;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_CHECKSUM;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.MORE_FLOWFILES;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.NO_DATA_FRAME;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.NO_MORE_FLOWFILES;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REJECT_CHECKSUM;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.SKIP_SPACE_CHECK;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.SPACE_AVAILABLE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class TestStandardLoadBalanceProtocol {
    private final LoadBalanceAuthorizer ALWAYS_AUTHORIZED = (sslSocket) -> sslSocket == null ? null : "authorized.mydomain.com";
    private FlowFileRepository flowFileRepo;
    private ContentRepository contentRepo;
    private ProvenanceRepository provenanceRepo;
    private FlowController flowController;
    private LoadBalancedFlowFileQueue flowFileQueue;

    private List<RepositoryRecord> flowFileRepoUpdateRecords;
    private List<ProvenanceEventRecord> provRepoUpdateRecords;
    private List<FlowFileRecord> flowFileQueuePutRecords;
    private List<FlowFileRecord> flowFileQueueReceiveRecords;

    private ConcurrentMap<ContentClaim, byte[]> claimContents;


    @Before
    public void setup() throws IOException {
        flowFileQueuePutRecords = new ArrayList<>();
        flowFileQueueReceiveRecords = new ArrayList<>();
        flowFileRepoUpdateRecords = new ArrayList<>();
        provRepoUpdateRecords = new ArrayList<>();

        flowFileRepo = Mockito.mock(FlowFileRepository.class);
        contentRepo = Mockito.mock(ContentRepository.class);
        provenanceRepo = Mockito.mock(ProvenanceRepository.class);
        flowController = Mockito.mock(FlowController.class);
        claimContents = new ConcurrentHashMap<>();

        Mockito.doAnswer(new Answer<ContentClaim>() {
            @Override
            public ContentClaim answer(final InvocationOnMock invocation) throws Throwable {
                final ContentClaim contentClaim = Mockito.mock(ContentClaim.class);
                final ResourceClaim resourceClaim = Mockito.mock(ResourceClaim.class);
                when(contentClaim.getResourceClaim()).thenReturn(resourceClaim);
                return contentClaim;
            }
        }).when(contentRepo).create(Mockito.anyBoolean());

        Mockito.doAnswer(new Answer<OutputStream>() {
            @Override
            public OutputStream answer(final InvocationOnMock invocation) throws Throwable {
                final ContentClaim contentClaim = invocation.getArgumentAt(0, ContentClaim.class);

                final ByteArrayOutputStream baos = new ByteArrayOutputStream() {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        claimContents.put(contentClaim, toByteArray());
                    }
                };

                return baos;
            }
        }).when(contentRepo).write(Mockito.any(ContentClaim.class));

        final Connection connection = Mockito.mock(Connection.class);
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        when(flowManager.getConnection(Mockito.anyString())).thenReturn(connection);
        when(flowController.getFlowManager()).thenReturn(flowManager);

        flowFileQueue = Mockito.mock(LoadBalancedFlowFileQueue.class);
        when(flowFileQueue.getLoadBalanceCompression()).thenReturn(LoadBalanceCompression.DO_NOT_COMPRESS);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                flowFileQueuePutRecords.addAll(invocation.getArgumentAt(0, Collection.class));
                return null;
            }
        }).when(flowFileQueue).putAll(anyCollection());

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                flowFileQueueReceiveRecords.addAll(invocation.getArgumentAt(0, Collection.class));
                return null;
            }
        }).when(flowFileQueue).receiveFromPeer(anyCollection());

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                flowFileRepoUpdateRecords.addAll(invocation.getArgumentAt(0, Collection.class));
                return null;
            }
        }).when(flowFileRepo).updateRepository(anyCollection());

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                provRepoUpdateRecords.addAll(invocation.getArgumentAt(0, Collection.class));
                return null;
            }
        }).when(provenanceRepo).registerEvents(anyCollection());
    }


    @Test
    public void testSimpleFlowFileTransaction() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "A");
        attributes.put("uuid", "unit-test-id");
        attributes.put("b", "B");

        dos.write(CHECK_SPACE);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);
        writeContent("hello".getBytes(), dos);
        dos.write(NO_MORE_FLOWFILES);

        dos.writeLong(checksum.getValue());
        dos.write(COMPLETE_TRANSACTION);

        protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(3, serverResponse.length);
        assertEquals(SPACE_AVAILABLE, serverResponse[0]);
        assertEquals(CONFIRM_CHECKSUM, serverResponse[1]);
        assertEquals(CONFIRM_COMPLETE_TRANSACTION, serverResponse[2]);

        assertEquals(1, claimContents.size());
        final byte[] firstFlowFileContent = claimContents.values().iterator().next();
        assertArrayEquals("hello".getBytes(), firstFlowFileContent);

        Mockito.verify(flowFileRepo, times(1)).updateRepository(anyCollection());
        Mockito.verify(provenanceRepo, times(1)).registerEvents(anyList());
        Mockito.verify(flowFileQueue, times(0)).putAll(anyCollection());
        Mockito.verify(flowFileQueue, times(1)).receiveFromPeer(anyCollection());
    }

    @Test
    public void testMultipleFlowFiles() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "A");
        attributes.put("uuid", "unit-test-id");
        attributes.put("b", "B");

        // Send 4 FlowFiles.
        dos.write(CHECK_SPACE);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);
        writeContent("hello".getBytes(), dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-2"), dos);
        writeContent(null, dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-3"), dos);
        writeContent("greetings".getBytes(), dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-4"), dos);
        writeContent(new byte[0], dos);

        dos.write(NO_MORE_FLOWFILES);

        dos.writeLong(checksum.getValue());
        dos.write(COMPLETE_TRANSACTION);

        protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(3, serverResponse.length);
        assertEquals(SPACE_AVAILABLE, serverResponse[0]);
        assertEquals(CONFIRM_CHECKSUM, serverResponse[1]);
        assertEquals(CONFIRM_COMPLETE_TRANSACTION, serverResponse[2]);

        assertEquals(1, claimContents.size());
        final byte[] bytes = claimContents.values().iterator().next();
        assertTrue(Arrays.equals("hellogreetings".getBytes(), bytes) || Arrays.equals("greetingshello".getBytes(), bytes));

        assertEquals(4, flowFileRepoUpdateRecords.size());
        assertEquals(4, provRepoUpdateRecords.size());
        assertEquals(0, flowFileQueuePutRecords.size());
        assertEquals(4, flowFileQueueReceiveRecords.size());

        assertTrue(provRepoUpdateRecords.stream().allMatch(event -> event.getEventType() == ProvenanceEventType.RECEIVE));
    }


    @Test
    public void testMultipleFlowFilesWithoutCheckingSpace() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "A");
        attributes.put("uuid", "unit-test-id");
        attributes.put("b", "B");

        // Send 4 FlowFiles.
        dos.write(SKIP_SPACE_CHECK);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);
        writeContent("hello".getBytes(), dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-2"), dos);
        writeContent(null, dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-3"), dos);
        writeContent("greetings".getBytes(), dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-4"), dos);
        writeContent(new byte[0], dos);

        dos.write(NO_MORE_FLOWFILES);

        dos.writeLong(checksum.getValue());
        dos.write(COMPLETE_TRANSACTION);

        protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(2, serverResponse.length);
        assertEquals(CONFIRM_CHECKSUM, serverResponse[0]);
        assertEquals(CONFIRM_COMPLETE_TRANSACTION, serverResponse[1]);

        assertEquals(1, claimContents.size());
        final byte[] bytes = claimContents.values().iterator().next();
        assertTrue(Arrays.equals("hellogreetings".getBytes(), bytes) || Arrays.equals("greetingshello".getBytes(), bytes));

        assertEquals(4, flowFileRepoUpdateRecords.size());
        assertEquals(4, provRepoUpdateRecords.size());
        assertEquals(0, flowFileQueuePutRecords.size());
        assertEquals(4, flowFileQueueReceiveRecords.size());

        assertTrue(provRepoUpdateRecords.stream().allMatch(event -> event.getEventType() == ProvenanceEventType.RECEIVE));
    }

    @Test
    public void testEofExceptionMultipleFlowFiles() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "A");
        attributes.put("uuid", "unit-test-id");
        attributes.put("b", "B");

        // Send 4 FlowFiles.
        dos.write(CHECK_SPACE);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);
        writeContent("hello".getBytes(), dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-2"), dos);
        writeContent(null, dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-3"), dos);
        writeContent("greetings".getBytes(), dos);

        dos.write(MORE_FLOWFILES);
        writeAttributes(Collections.singletonMap("uuid", "unit-test-id-4"), dos);
        writeContent(new byte[0], dos);

        dos.flush();
        dos.close();

        try {
            protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);
            Assert.fail("Expected EOFException but none was thrown");
        } catch (final EOFException eof) {
            // expected
        }

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(1, serverResponse.length);
        assertEquals(SPACE_AVAILABLE, serverResponse[0]);

        assertEquals(1, claimContents.size());
        assertArrayEquals("hellogreetings".getBytes(), claimContents.values().iterator().next());

        assertEquals(0, flowFileRepoUpdateRecords.size());
        assertEquals(0, provRepoUpdateRecords.size());
        assertEquals(0, flowFileQueuePutRecords.size());
    }

    @Test
    public void testBadChecksum() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid", "unit-test-id");

        dos.write(CHECK_SPACE);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);
        writeContent("hello".getBytes(), dos);
        dos.write(NO_MORE_FLOWFILES);

        dos.writeLong(1L); // Write bad checksum.
        dos.write(COMPLETE_TRANSACTION);

        try {
            protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);
            Assert.fail("Expected TransactionAbortedException but none was thrown");
        } catch (final TransactionAbortedException e) {
            // expected
        }

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(2, serverResponse.length);
        assertEquals(SPACE_AVAILABLE, serverResponse[0]);
        assertEquals(REJECT_CHECKSUM, serverResponse[1]);

        assertEquals(1, claimContents.size());
        final byte[] firstFlowFileContent = claimContents.values().iterator().next();
        assertArrayEquals("hello".getBytes(), firstFlowFileContent);

        Mockito.verify(flowFileRepo, times(0)).updateRepository(anyCollection());
        Mockito.verify(provenanceRepo, times(0)).registerEvents(anyList());
        Mockito.verify(flowFileQueue, times(0)).putAll(anyCollection());
        Mockito.verify(contentRepo, times(1)).incrementClaimaintCount(claimContents.keySet().iterator().next());
        Mockito.verify(contentRepo, times(2)).decrementClaimantCount(claimContents.keySet().iterator().next());
        Mockito.verify(contentRepo, times(1)).remove(claimContents.keySet().iterator().next());
    }

    @Test
    public void testEofWritingContent() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid", "unit-test-id");

        dos.write(CHECK_SPACE);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);

        // Indicate 45 byte data frame, then stop after 5 bytes.
        dos.write(DATA_FRAME_FOLLOWS);
        dos.writeShort(45);
        dos.write("hello".getBytes());
        dos.flush();
        dos.close();

        try {
            protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);
            Assert.fail("Expected EOFException but none was thrown");
        } catch (final EOFException e) {
            // expected
        }

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(1, serverResponse.length);
        assertEquals(SPACE_AVAILABLE, serverResponse[0]);

        assertEquals(1, claimContents.size());
        final byte[] firstFlowFileContent = claimContents.values().iterator().next();
        assertArrayEquals(new byte[0], firstFlowFileContent);

        Mockito.verify(flowFileRepo, times(0)).updateRepository(anyCollection());
        Mockito.verify(provenanceRepo, times(0)).registerEvents(anyList());
        Mockito.verify(flowFileQueue, times(0)).putAll(anyCollection());
        Mockito.verify(contentRepo, times(0)).incrementClaimaintCount(claimContents.keySet().iterator().next());
        Mockito.verify(contentRepo, times(0)).decrementClaimantCount(claimContents.keySet().iterator().next());
        Mockito.verify(contentRepo, times(1)).remove(claimContents.keySet().iterator().next());
    }

    @Test
    public void testAbortAfterChecksumConfirmation() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid", "unit-test-id");

        dos.write(CHECK_SPACE);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);
        writeContent("hello".getBytes(), dos);
        dos.write(NO_MORE_FLOWFILES);

        dos.writeLong(checksum.getValue());
        dos.write(ABORT_TRANSACTION);

        try {
            protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);
            Assert.fail("Expected TransactionAbortedException but none was thrown");
        } catch (final TransactionAbortedException e) {
            // expected
        }

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(2, serverResponse.length);
        assertEquals(SPACE_AVAILABLE, serverResponse[0]);
        assertEquals(CONFIRM_CHECKSUM, serverResponse[1]);

        assertEquals(1, claimContents.size());
        final byte[] firstFlowFileContent = claimContents.values().iterator().next();
        assertArrayEquals("hello".getBytes(), firstFlowFileContent);

        Mockito.verify(flowFileRepo, times(0)).updateRepository(anyCollection());
        Mockito.verify(provenanceRepo, times(0)).registerEvents(anyList());
        Mockito.verify(flowFileQueue, times(0)).putAll(anyCollection());
        Mockito.verify(contentRepo, times(1)).incrementClaimaintCount(claimContents.keySet().iterator().next());
        Mockito.verify(contentRepo, times(2)).decrementClaimantCount(claimContents.keySet().iterator().next());
        Mockito.verify(contentRepo, times(1)).remove(claimContents.keySet().iterator().next());
    }

    @Test
    public void testFlowFileNoContent() throws IOException {
        final StandardLoadBalanceProtocol protocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepo, provenanceRepo, flowController, ALWAYS_AUTHORIZED);

        final PipedInputStream serverInput = new PipedInputStream();
        final PipedOutputStream serverContentSource = new PipedOutputStream();
        serverInput.connect(serverContentSource);

        final ByteArrayOutputStream serverOutput = new ByteArrayOutputStream();

        // Write connection ID
        final Checksum checksum = new CRC32();
        final OutputStream checkedOutput = new CheckedOutputStream(serverContentSource, checksum);
        final DataOutputStream dos = new DataOutputStream(checkedOutput);
        dos.writeUTF("unit-test-connection-id");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid", "unit-test-id");

        dos.write(CHECK_SPACE);
        dos.write(MORE_FLOWFILES);
        writeAttributes(attributes, dos);
        writeContent(null, dos);
        dos.write(NO_MORE_FLOWFILES);

        dos.writeLong(checksum.getValue());
        dos.write(COMPLETE_TRANSACTION);

        protocol.receiveFlowFiles(serverInput, serverOutput, "Unit Test", 1);

        final byte[] serverResponse = serverOutput.toByteArray();
        assertEquals(3, serverResponse.length);
        assertEquals(SPACE_AVAILABLE, serverResponse[0]);
        assertEquals(CONFIRM_CHECKSUM, serverResponse[1]);
        assertEquals(CONFIRM_COMPLETE_TRANSACTION, serverResponse[2]);

        assertEquals(1, claimContents.size());
        assertEquals(0, claimContents.values().iterator().next().length);

        Mockito.verify(flowFileRepo, times(1)).updateRepository(anyCollection());
        Mockito.verify(provenanceRepo, times(1)).registerEvents(anyList());
        Mockito.verify(flowFileQueue, times(0)).putAll(anyCollection());
        Mockito.verify(flowFileQueue, times(1)).receiveFromPeer(anyCollection());
    }

    private void writeAttributes(final Map<String, String> attributes, final DataOutputStream dos) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(attributes.size());

            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                final byte[] key = entry.getKey().getBytes();
                out.writeInt(key.length);
                out.write(key);

                final byte[] value = entry.getValue().getBytes();
                out.writeInt(value.length);
                out.write(value);
            }

            out.writeLong(0L); // lineage start date
            out.writeLong(0L); // entry date

            dos.writeInt(baos.size());
            baos.writeTo(dos);
        }

    }

    private void writeContent(final byte[] content, final DataOutputStream out) throws IOException {
        if (content == null) {
            out.write(NO_DATA_FRAME);
            return;
        }

        int iterations = content.length / 65535;
        if (content.length % 65535 > 0) {
            iterations++;
        }

        for (int i=0; i < iterations; i++) {
            final int offset = i * 65536;
            final int length = Math.min(content.length - offset, 65535);

            out.write(DATA_FRAME_FOLLOWS);
            out.writeShort(length);
            out.write(content, offset, length);
        }

        out.write(NO_DATA_FRAME);
    }
}
