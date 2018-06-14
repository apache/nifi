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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.MockSwapManager;
import org.apache.nifi.controller.queue.BlockingSwappablePriorityQueue;
import org.apache.nifi.controller.queue.DropFlowFileAction;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceClient;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceTransaction;
import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTransferFlowFiles {
    private MockSwapManager swapManager;
    private LoadBalancedFlowFileQueue flowFileQueue;
    private LoadBalanceClient loadBalanceClient;
    private FlowFileRepository flowFileRepo;
    private ProvenanceEventRepository provRepo;
    private ContentRepository contentRepo;
    private BlockingSwappablePriorityQueue queue;
    private TransferFlowFiles transfer;
    private MockTransferFailureDestination failureDestination;

    private Set<FlowFileRecord> expiredRecords;
    private List<RepositoryRecord> abortedRecords;
    private final long expirationMillis = 500000;

    @Before
    public void setup() {
        final DropFlowFileAction dropAction = (flowFiles, requestor) -> {
            return new QueueSize(flowFiles.size(), flowFiles.stream().mapToLong(FlowFileRecord::getSize).sum());
        };

        loadBalanceClient = mock(LoadBalanceClient.class);

        swapManager = new MockSwapManager();
        flowFileQueue = mock(LoadBalancedFlowFileQueue.class);

        flowFileRepo = mock(FlowFileRepository.class);
        provRepo = mock(ProvenanceEventRepository.class);
        contentRepo = mock(ContentRepository.class);

        when(flowFileQueue.getIdentifier()).thenReturn("unit-test");

        failureDestination = new MockTransferFailureDestination();

        queue = new BlockingSwappablePriorityQueue(swapManager, 10000, EventReporter.NO_OP, flowFileQueue, dropAction, "local");
        transfer = new TransferFlowFiles(queue, flowFileQueue, loadBalanceClient, failureDestination, EventReporter.NO_OP, flowFileRepo, provRepo, contentRepo);

        expiredRecords = new HashSet<>();
        abortedRecords = new ArrayList<>();
    }

    @Test
    public void testNoTransactionCreatedWithoutFlowFiles() throws InterruptedException, IOException {
        transfer.transferFlowFiles(expiredRecords, abortedRecords, expirationMillis);

        verify(loadBalanceClient, times(0)).createTransaction(anyString());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testLifecycleWithMultipleFlowFiles() throws InterruptedException, IOException {
        final ContentClaim contentClaim = createContentClaim("hello".getBytes());

        final FlowFileRecord flowFile = new MockFlowFileRecord(5) {
            @Override
            public ContentClaim getContentClaim() {
                return contentClaim;
            }
        };

        final int flowFileCount = 10;

        for (int i = 0; i < flowFileCount; i++) {
            queue.put(flowFile);
        }

        assertEquals(10, queue.size().getObjectCount());

        // Verify that we get the correct number of Provenance Events
        doAnswer(invocation -> {
            final Iterable<ProvenanceEventRecord> recordIterable = invocation.getArgumentAt(0, Iterable.class);

            final Map<ProvenanceEventType, List<ProvenanceEventRecord>> eventMap = StreamSupport.stream(recordIterable.spliterator(), false)
                .collect(Collectors.groupingBy(ProvenanceEventRecord::getEventType));

            assertEquals(2, eventMap.size());
            assertEquals(flowFileCount, eventMap.get(ProvenanceEventType.SEND).size());
            assertEquals(flowFileCount, eventMap.get(ProvenanceEventType.DROP).size());

            return null;
        }).when(provRepo).registerEvents(any(Iterable.class));

        // Verify that we get the correct number of updates to the FlowFile Repo and that they are of the correct type.
        doAnswer(invocation -> {
            final Collection<RepositoryRecord> updates = invocation.getArgumentAt(0, Collection.class);
            assertEquals(flowFileCount, updates.size());

            updates.stream().forEach(record -> assertEquals(RepositoryRecordType.DELETE, record.getType()));
            return null;
        }).when(flowFileRepo).updateRepository(Mockito.anyCollection());

        // Mock out the transaction so that we verify the FlowFiles and their content are properly given to the transaction.
        final LoadBalanceTransaction transaction = mock(LoadBalanceTransaction.class);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                assertSame(flowFile, invocation.getArgumentAt(0, FlowFileRecord.class));

                final byte[] buffer = new byte[5];
                final InputStream in = invocation.getArgumentAt(1, InputStream.class);
                StreamUtils.fillBuffer(in, buffer);

                assertEquals(-1, in.read());
                assertEquals("hello", new String(buffer));

                return null;
            }
        }).when(transaction).send(any(FlowFileRecord.class), any(InputStream.class));

        // Provide our mocked transaction when creating one from client.
        when(loadBalanceClient.createTransaction(anyString())).thenReturn(transaction);

        // trigger transfer
        transfer.transferFlowFiles(expiredRecords, abortedRecords, expirationMillis);

        // Ensure that we create a single transaction, send all FlowFiles, then complete it.
        verify(loadBalanceClient, times(1)).createTransaction(anyString());
        verify(transaction, times(10)).send(any(FlowFileRecord.class), any(InputStream.class));
        verify(transaction, times(1)).complete();

        // Ensure that we update the flowfile and provenance repositories properly.
        verify(flowFileRepo, times(1)).updateRepository(Mockito.anyCollection());
        verify(provRepo, times(1)).registerEvents(any(Iterable.class));

        // Ensure that we properly acknowledge the FlowFiles so that the queue's count is accurate.
        assertEquals(0, queue.size().getObjectCount());
    }

    private ContentClaim createContentClaim(final byte[] bytes) throws IOException {
        final ResourceClaim resourceClaim = mock(ResourceClaim.class);
        when(resourceClaim.getContainer()).thenReturn("container");
        when(resourceClaim.getSection()).thenReturn("section");
        when(resourceClaim.getId()).thenReturn("identifier");

        final ContentClaim contentClaim = mock(ContentClaim.class);
        when(contentClaim.getResourceClaim()).thenReturn(resourceClaim);

        if (bytes != null) {
            when(contentRepo.read(contentClaim)).thenAnswer(invocation -> new ByteArrayInputStream(bytes));
        }

        return contentClaim;
    }

    /**
     * If we have a ContentNotFound occur in the middle of a Transaction, we should continue the transaction and just skip over the one FlowFile.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testContentNotFound() throws IOException, InterruptedException {
        final ContentClaim validContentClaim = createContentClaim("hello".getBytes());

        final ContentClaim invalidContentClaim = createContentClaim(null);
        Mockito.doThrow(new ContentNotFoundException(invalidContentClaim))
            .when(contentRepo).read(invalidContentClaim);

        final FlowFileRecord flowFileValidClaim = new MockFlowFileRecord() {
            @Override
            public ContentClaim getContentClaim() {
                return validContentClaim;
            }
        };

        final FlowFileRecord flowFileInvalidClaim = new MockFlowFileRecord() {
            @Override
            public ContentClaim getContentClaim() {
                return invalidContentClaim;
            }
        };

        queue.put(flowFileValidClaim);
        queue.put(flowFileInvalidClaim);
        queue.put(flowFileValidClaim);

        // Verify that we get the correct number of Provenance Events and that they are all SEND events.
        doAnswer(invocation -> {
            final Iterable<ProvenanceEventRecord> recordIterable = invocation.getArgumentAt(0, Iterable.class);

            final Map<ProvenanceEventType, List<ProvenanceEventRecord>> eventMap = StreamSupport.stream(recordIterable.spliterator(), false)
                .collect(Collectors.groupingBy(ProvenanceEventRecord::getEventType));

            assertEquals(2, eventMap.size());
            assertEquals(3, eventMap.get(ProvenanceEventType.DROP).size());
            assertEquals(2, eventMap.get(ProvenanceEventType.SEND).size());
            return null;
        }).when(provRepo).registerEvents(any(Iterable.class));

        // Verify that we get the correct number of updates to the FlowFile Repo and that they are of the correct type.
        doAnswer(invocation -> {
            final Collection<RepositoryRecord> updates = invocation.getArgumentAt(0, Collection.class);

            final Map<RepositoryRecordType, List<RepositoryRecord>> recordMap = updates.stream()
                .collect(Collectors.groupingBy(RepositoryRecord::getType));
            assertEquals(2, recordMap.size());

            assertEquals(2, recordMap.get(RepositoryRecordType.DELETE).size());
            assertEquals(1, recordMap.get(RepositoryRecordType.CONTENTMISSING).size());

            return null;
        }).when(flowFileRepo).updateRepository(Mockito.anyCollection());

        final LoadBalanceTransaction transaction = mock(LoadBalanceTransaction.class);
        when(loadBalanceClient.createTransaction(anyString())).thenReturn(transaction);

        // trigger transfer
        transfer.transferFlowFiles(expiredRecords, abortedRecords, expirationMillis);

        // Ensure that we create a single transaction, send all FlowFiles, then complete it.
        verify(loadBalanceClient, times(1)).createTransaction(anyString());
        verify(transaction, times(2)).send(any(FlowFileRecord.class), any(InputStream.class));
        verify(transaction, times(1)).complete();

        // Ensure that we update the flowfile and provenance repositories properly.
        verify(flowFileRepo, times(1)).updateRepository(Mockito.anyCollection());
        verify(provRepo, times(1)).registerEvents(any(Iterable.class));

        // Ensure that we properly acknowledge the FlowFiles so that the queue's count is accurate.
        assertEquals(0, queue.size().getObjectCount());
    }

    /**
     * If we run into an IOException we should abort the entire transaction.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testIOException() throws IOException, InterruptedException {
        final ContentClaim validContentClaim = createContentClaim("hello".getBytes());

        final ContentClaim ioExceptionContentClaim = createContentClaim(null);
        when(contentRepo.read(ioExceptionContentClaim)).thenAnswer(invocation -> new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Intentional unit test exception");
            }
        });

        final FlowFileRecord flowFileValidClaim = new MockFlowFileRecord() {
            @Override
            public ContentClaim getContentClaim() {
                return validContentClaim;
            }
        };

        final FlowFileRecord flowFileIoExceptionClaim = new MockFlowFileRecord() {
            @Override
            public ContentClaim getContentClaim() {
                return ioExceptionContentClaim;
            }
        };

        queue.put(flowFileValidClaim);
        queue.put(flowFileIoExceptionClaim);

        // Mock out a Transaction to read from the InputStream of a FlowFile when send() is called.
        final LoadBalanceTransaction transaction = mock(LoadBalanceTransaction.class);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final InputStream in = invocation.getArgumentAt(1, InputStream.class);
                in.read();
                return null;
            }
        }).when(transaction).send(any(FlowFileRecord.class), any(InputStream.class));

        when(loadBalanceClient.createTransaction(anyString())).thenReturn(transaction);

        // trigger transfer
        try {
            transfer.transferFlowFiles(expiredRecords, abortedRecords, expirationMillis);
            Assert.fail("transferFlowFiles did not throw IOE");
        } catch (final IOException ioe) {
            // Expected
        }

        // Ensure that we update the flowfile and provenance repositories properly.
        verify(flowFileRepo, times(0)).updateRepository(Mockito.anyCollection());
        verify(provRepo, times(0)).registerEvents(any(Iterable.class));

        // Ensure that we properly acknowledge the FlowFiles so that the queue's count is accurate.
        assertEquals(2, failureDestination.getFlowFilesTransferred().size());

        verify(transaction, times(0)).complete();
        verify(transaction, times(1)).abort();
    }
}
