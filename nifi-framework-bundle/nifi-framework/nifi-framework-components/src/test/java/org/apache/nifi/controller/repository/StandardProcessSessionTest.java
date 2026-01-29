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
import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimWriteCache;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.provenance.InternalProvenanceReporter;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardProcessSessionTest {
    private static final String BACKOFF_PERIOD = "5 s";

    private static final boolean APPEND_DISABLED = false;

    private static final long EXPECTED_BYTES = 32;

    private static final byte[] CONTENT = new byte[]{2};

    private static final long BYTES_READ = CONTENT.length;

    private static final long BYTES_WRITTEN = CONTENT.length;

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
    ArgumentCaptor<FlowFileEvent> flowFileEventCaptor;

    StandardProcessSession session;

    @BeforeEach
    void setSession() {
        when(repositoryContext.createProvenanceReporter(any(), any())).thenReturn(provenanceReporter);
        when(repositoryContext.createContentClaimWriteCache(isA(PerformanceTracker.class))).thenReturn(contentClaimWriteCache);
        when(repositoryContext.getConnectable()).thenReturn(connectable);
        when(connectable.getIdentifier()).thenReturn(Connectable.class.getSimpleName());

        session = new StandardProcessSession(repositoryContext, taskTermination, performanceTracker);
    }

    @Test
    void testExportToPathFlowFileEventBytes() throws IOException {
        setRepositoryContext();

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

    private void assertFlowFileEventMatched(final long bytesRead, final long bytesWritten) throws IOException {
        verify(flowFileEventRepository).updateRepository(flowFileEventCaptor.capture(), anyString());
        final FlowFileEvent flowFileEvent = flowFileEventCaptor.getValue();

        assertEquals(bytesRead, flowFileEvent.getBytesRead(), "Bytes read not matched");
        assertEquals(bytesWritten, flowFileEvent.getBytesWritten(), "Bytes written not matched");
    }

    private void setRepositoryContext() {
        when(repositoryContext.getContentRepository()).thenReturn(contentRepository);
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
