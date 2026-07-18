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
package org.apache.nifi.controller.repository;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.FlowFileActivity;
import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.ConnectionStatusEvent;
import org.apache.nifi.controller.metrics.GaugeRecord;
import org.apache.nifi.controller.metrics.ProcessSessionEvent;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.controller.status.LoadBalanceStatus;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.metrics.CommitTiming;
import org.apache.nifi.provenance.InternalProvenanceReporter;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StandardProcessSessionTest {
    private static final String BACKOFF_PERIOD = "5 s";

    private static final boolean APPEND_DISABLED = false;

    private static final long EXPECTED_BYTES = 32;

    private static final byte[] CONTENT = new byte[]{2};

    private static final long BYTES_READ = CONTENT.length;

    private static final long BYTES_WRITTEN = CONTENT.length;

    private static final String GAUGE_NAME = "freeMemory";

    private static final double GAUGE_VALUE = 64.5;

    private static final String INPUT_CONNECTION_ID = "input-connection-id";
    private static final String OUTPUT_CONNECTION_ID = "output-connection-id";
    private static final String BACK_PRESSURE_DATA_SIZE_THRESHOLD = "1 MB";
    private static final long BACK_PRESSURE_BYTES_THRESHOLD = 1048576;

    @Mock
    RepositoryContext repositoryContext;

    @Mock
    TaskTermination taskTermination;

    @Mock
    Connectable connectable;

    @Mock
    ContentClaimWriteCache contentClaimWriteCache;

    @Mock
    ContentClaim contentClaim;

    @Mock
    InternalProvenanceReporter provenanceReporter;

    @Mock
    ProvenanceRepository provenanceRepository;

    @Mock
    FlowFileRepository flowFileRepository;

    @Mock
    FlowFileEventRepository flowFileEventRepository;

    @Mock
    ContentRepository contentRepository;

    @Mock
    PerformanceTracker performanceTracker;

    @Captor
    ArgumentCaptor<ProcessSessionEvent> flowFileEventCaptor;

    @Captor
    ArgumentCaptor<ProcessSessionEvent> processSessionEventCaptor;

    @Captor
    ArgumentCaptor<GaugeRecord> gaugeRecordCaptor;

    @Captor
    ArgumentCaptor<ConnectionStatusEvent> connectionStatusEventCaptor;

    StandardProcessSession session;

    @BeforeEach
    void setSession() {
        when(repositoryContext.createProvenanceReporter(any(), any())).thenReturn(provenanceReporter);
        when(repositoryContext.createContentClaimWriteCache(isA(PerformanceTracker.class))).thenReturn(contentClaimWriteCache);
        when(repositoryContext.getConnectable()).thenReturn(connectable);
        when(connectable.getIdentifier()).thenReturn(Connectable.class.getSimpleName());
        final FlowFileActivity flowFileActivity = mock(FlowFileActivity.class);
        when(connectable.getFlowFileActivity()).thenReturn(flowFileActivity);

        session = new StandardProcessSession(repositoryContext, taskTermination, performanceTracker);
    }

    @Test
    void testGetTransferConnectionStatusEventsDisabled() {
        setRepositoryContext();
        when(repositoryContext.getContentRepository()).thenReturn(contentRepository);
        when(repositoryContext.isRecordConnectionStatusEventEnabled()).thenReturn(false);

        final Connection connection = mock(Connection.class);
        when(repositoryContext.getPollableConnections()).thenReturn(List.of(connection));
        final FlowFileRecord flowFileRecord = mock(FlowFileRecord.class);
        when(connection.poll(anySet())).thenReturn(flowFileRecord);
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getIdentifier()).thenReturn(INPUT_CONNECTION_ID);

        final FlowFile flowFile = session.get();
        assertNotNull(flowFile);
        session.transfer(flowFile);
        session.commit();

        verify(repositoryContext, never()).recordConnectionStatusEvent(connectionStatusEventCaptor.capture());
    }

    @Test
    void testGetTransferConnectionStatusEvents() {
        setRepositoryContext();
        when(repositoryContext.getContentRepository()).thenReturn(contentRepository);
        when(repositoryContext.isRecordConnectionStatusEventEnabled()).thenReturn(true);

        final Connection connection = mock(Connection.class);
        when(repositoryContext.getPollableConnections()).thenReturn(List.of(connection));
        final FlowFileRecord flowFileRecord = mock(FlowFileRecord.class);
        when(connection.poll(anySet())).thenReturn(flowFileRecord);
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getIdentifier()).thenReturn(INPUT_CONNECTION_ID);

        final FlowFile flowFile = session.get();
        assertNotNull(flowFile);

        final Connection outputConnection = mock(Connection.class);
        when(outputConnection.getIdentifier()).thenReturn(OUTPUT_CONNECTION_ID);
        final FlowFileQueue outputFlowFileQueue = mock(FlowFileQueue.class);
        when(outputFlowFileQueue.getBackPressureDataSizeThreshold()).thenReturn(BACK_PRESSURE_DATA_SIZE_THRESHOLD);
        when(outputConnection.getFlowFileQueue()).thenReturn(outputFlowFileQueue);
        final QueueSize outputQueueSize = mock(QueueSize.class);
        when(outputFlowFileQueue.size()).thenReturn(outputQueueSize);
        when(outputFlowFileQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.FLOWFILE_AVAILABLE);

        final Relationship relationship = new Relationship.Builder().name(Relationship.class.getSimpleName()).build();
        when(repositoryContext.getConnections(eq(relationship))).thenReturn(List.of(outputConnection));
        session.transfer(flowFile, relationship);

        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn(BACK_PRESSURE_DATA_SIZE_THRESHOLD);
        final QueueSize queueSize = mock(QueueSize.class);
        final int objectCount = Integer.MAX_VALUE;
        when(queueSize.getObjectCount()).thenReturn(objectCount);
        final long byteCount = Long.MAX_VALUE;
        when(queueSize.getByteCount()).thenReturn(byteCount);
        when(flowFileQueue.size()).thenReturn(queueSize);
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.ROUND_ROBIN);
        when(flowFileQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.FLOWFILE_AVAILABLE);

        session.commit();

        verify(repositoryContext, times(2)).recordConnectionStatusEvent(connectionStatusEventCaptor.capture());
        final List<ConnectionStatusEvent> events = connectionStatusEventCaptor.getAllValues();

        final ConnectionStatusEvent firstConnectionStatusEvent = events.getFirst();
        final ComponentMetricContext componentMetricContext = firstConnectionStatusEvent.getComponentMetricContext();
        assertEquals(INPUT_CONNECTION_ID, componentMetricContext.id());
        assertEquals(BACK_PRESSURE_BYTES_THRESHOLD, firstConnectionStatusEvent.getBackPressureBytesThreshold());
        assertEquals(objectCount, firstConnectionStatusEvent.getQueuedCount());
        assertEquals(byteCount, firstConnectionStatusEvent.getQueuedBytes());
        assertEquals(LoadBalanceStatus.LOAD_BALANCE_INACTIVE, firstConnectionStatusEvent.getLoadBalanceStatus());

        final ConnectionStatusEvent secondConnectionStatusEvent = events.getLast();
        final ComponentMetricContext secondComponentMetricContext = secondConnectionStatusEvent.getComponentMetricContext();
        assertEquals(OUTPUT_CONNECTION_ID, secondComponentMetricContext.id());
    }

    @Test
    void testBatchedCheckpointRetainsConnectionMetricContext() {
        setRepositoryContext();
        when(repositoryContext.getContentRepository()).thenReturn(contentRepository);
        when(repositoryContext.isRecordConnectionStatusEventEnabled()).thenReturn(true);

        final Connection connection = mock(Connection.class);
        when(repositoryContext.getPollableConnections()).thenReturn(List.of(connection));
        final FlowFileRecord flowFileRecord = mock(FlowFileRecord.class);
        when(connection.poll(anySet())).thenReturn(flowFileRecord);
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getIdentifier()).thenReturn(INPUT_CONNECTION_ID);
        when(connection.getName()).thenReturn("Connection Name");

        final FlowFile flowFile = session.get();
        assertNotNull(flowFile);

        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn(BACK_PRESSURE_DATA_SIZE_THRESHOLD);
        final QueueSize queueSize = mock(QueueSize.class);
        when(flowFileQueue.size()).thenReturn(queueSize);
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE);
        when(flowFileQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.FLOWFILE_AVAILABLE);

        session.remove(flowFile);
        session.checkpoint();
        session.commit();

        verify(repositoryContext, times(1)).recordConnectionStatusEvent(connectionStatusEventCaptor.capture());
        final ConnectionStatusEvent connectionStatusEvent = connectionStatusEventCaptor.getValue();
        assertEquals("Connection Name", connectionStatusEvent.getComponentMetricContext().name());
    }

    @Test
    void testMigrateTracksConnectionStatusEventForNewOwner() {
        setRepositoryContext();
        when(repositoryContext.getContentRepository()).thenReturn(contentRepository);
        when(repositoryContext.isRecordConnectionStatusEventEnabled()).thenReturn(true);

        final Connection connection = mock(Connection.class);
        when(repositoryContext.getPollableConnections()).thenReturn(List.of(connection));
        final FlowFileRecord flowFileRecord = mock(FlowFileRecord.class);
        when(connection.poll(anySet())).thenReturn(flowFileRecord);
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getIdentifier()).thenReturn(INPUT_CONNECTION_ID);
        when(flowFileQueue.getIdentifier()).thenReturn(INPUT_CONNECTION_ID);

        final FlowFile flowFile = session.get();
        assertNotNull(flowFile);

        final StandardProcessSession newOwner = new StandardProcessSession(repositoryContext, taskTermination, performanceTracker);
        session.migrate(newOwner);

        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn(BACK_PRESSURE_DATA_SIZE_THRESHOLD);
        final QueueSize queueSize = mock(QueueSize.class);
        final int objectCount = Integer.MAX_VALUE;
        when(queueSize.getObjectCount()).thenReturn(objectCount);
        final long byteCount = Long.MAX_VALUE;
        when(queueSize.getByteCount()).thenReturn(byteCount);
        when(flowFileQueue.size()).thenReturn(queueSize);
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE);
        when(flowFileQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.FLOWFILE_AVAILABLE);

        newOwner.remove(flowFile);
        newOwner.commit();

        verify(repositoryContext, times(1)).recordConnectionStatusEvent(connectionStatusEventCaptor.capture());
        final ConnectionStatusEvent connectionStatusEvent = connectionStatusEventCaptor.getValue();
        assertEquals(INPUT_CONNECTION_ID, connectionStatusEvent.getComponentMetricContext().id());
        assertEquals(BACK_PRESSURE_BYTES_THRESHOLD, connectionStatusEvent.getBackPressureBytesThreshold());
        assertEquals(objectCount, connectionStatusEvent.getQueuedCount());
        assertEquals(byteCount, connectionStatusEvent.getQueuedBytes());
        assertEquals(LoadBalanceStatus.LOAD_BALANCE_NOT_CONFIGURED, connectionStatusEvent.getLoadBalanceStatus());
        assertEquals(FlowFileAvailability.FLOWFILE_AVAILABLE, connectionStatusEvent.getFlowFileAvailability());
    }

    @Test
    void testExportToPathFlowFileEventBytes() throws IOException {
        setRepositoryContext();
        when(repositoryContext.getContentRepository()).thenReturn(contentRepository);

        final Path destination = getDestination();
        when(contentRepository.exportTo(isNull(), eq(destination), eq(APPEND_DISABLED), anyLong(), anyLong())).thenReturn(EXPECTED_BYTES);

        final FlowFile flowFile = session.create();
        session.exportTo(flowFile, destination, APPEND_DISABLED);
        session.remove(flowFile);
        session.commit();

        assertFlowFileEventMatched(EXPECTED_BYTES, 0);
    }

    @Test
    void testExportToOutputStreamFlowFileEventBytes() throws IOException {
        setRepositoryContext();
        when(repositoryContext.getContentRepository()).thenReturn(contentRepository);

        FlowFile flowFile = session.create();

        final ByteArrayOutputStream claimOutputStream = new ByteArrayOutputStream();
        when(contentClaimWriteCache.getContentClaim()).thenReturn(contentClaim);
        when(contentClaimWriteCache.write(eq(contentClaim))).thenReturn(claimOutputStream);

        flowFile = session.write(flowFile, outputStream -> outputStream.write(CONTENT));

        final ByteArrayInputStream contentInputStream = new ByteArrayInputStream(CONTENT);
        when(contentRepository.read(eq(contentClaim))).thenReturn(contentInputStream);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        session.exportTo(flowFile, outputStream);
        session.remove(flowFile);
        session.commit();

        assertFlowFileEventMatched(BYTES_READ, BYTES_WRITTEN);
    }

    @Test
    void testRecordGaugeNow() {
        session.recordGauge(GAUGE_NAME, GAUGE_VALUE, CommitTiming.NOW);

        verify(repositoryContext).recordGauge(gaugeRecordCaptor.capture());
        final GaugeRecord gaugeRecord = gaugeRecordCaptor.getValue();

        assertEquals(GAUGE_NAME, gaugeRecord.name());
        assertEquals(GAUGE_VALUE, gaugeRecord.value());
    }

    @Test
    void testRecordGaugeSessionCommitted() {
        session.recordGauge(GAUGE_NAME, GAUGE_VALUE, CommitTiming.SESSION_COMMITTED);

        setRepositoryContext();
        session.commit();

        verify(repositoryContext).recordGauge(gaugeRecordCaptor.capture());
        final GaugeRecord gaugeRecord = gaugeRecordCaptor.getValue();

        assertEquals(GAUGE_NAME, gaugeRecord.name());
        assertEquals(GAUGE_VALUE, gaugeRecord.value());
    }

    @Test
    void testCreateLineage() {
        final long firstFlowFileId = 1;
        final long secondFlowFileId = 2;
        when(repositoryContext.getNextFlowFileSequence()).thenReturn(firstFlowFileId, secondFlowFileId);

        final FlowFile firstFlowFile = session.create();

        assertNotNull(firstFlowFile);
        assertNotEquals(0, firstFlowFile.getLineageStartDate());
        assertEquals(firstFlowFile.getEntryDate(), firstFlowFile.getLineageStartDate());
        assertEquals(firstFlowFileId, firstFlowFile.getId());
        assertEquals(firstFlowFileId, firstFlowFile.getLineageStartIndex());

        final FlowFile secondFlowFile = session.create();
        assertNotNull(secondFlowFile);
        assertEquals(secondFlowFileId, secondFlowFile.getId());
        assertEquals(secondFlowFileId, secondFlowFile.getLineageStartIndex());
    }

    private void assertFlowFileEventMatched(final long bytesRead, final long bytesWritten) throws IOException {
        verify(flowFileEventRepository).updateRepository(flowFileEventCaptor.capture());
        final ProcessSessionEvent flowFileEvent = flowFileEventCaptor.getValue();

        assertEquals(bytesRead, flowFileEvent.getBytesRead(), "Bytes read not matched");
        assertEquals(bytesWritten, flowFileEvent.getBytesWritten(), "Bytes written not matched");

        verify(repositoryContext).recordProcessSessionEvent(processSessionEventCaptor.capture());
        final ProcessSessionEvent processSessionEvent = processSessionEventCaptor.getValue();

        assertEquals(bytesRead, processSessionEvent.getBytesRead(), "Process Session Bytes read not matched");
        assertEquals(bytesWritten, processSessionEvent.getBytesWritten(), "Process Session Bytes written not matched");
    }

    private void setRepositoryContext() {
        when(repositoryContext.getProvenanceRepository()).thenReturn(provenanceRepository);
        when(repositoryContext.getFlowFileRepository()).thenReturn(flowFileRepository);
        when(repositoryContext.getFlowFileEventRepository()).thenReturn(flowFileEventRepository);
        when(connectable.getMaxBackoffPeriod()).thenReturn(BACKOFF_PERIOD);
    }

    private Path getDestination() throws IOException {
        final Path destination = Files.createTempFile(StandardProcessSessionTest.class.getSimpleName(), ProcessSession.class.getSimpleName());
        destination.toFile().deleteOnExit();
        return destination;
    }
}
