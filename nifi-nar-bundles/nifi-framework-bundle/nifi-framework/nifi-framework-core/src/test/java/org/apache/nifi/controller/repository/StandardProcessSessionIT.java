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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.NopConnectionEventListener;
import org.apache.nifi.controller.queue.StandardFlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.MissingFlowFileException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardProcessSessionIT {

    private StandardProcessSession session;
    private MockContentRepository contentRepo;
    private FlowFileQueue flowFileQueue;
    private StandardRepositoryContext context;
    private Connectable connectable;
    private MockStateManager stateManager;

    private ProvenanceEventRepository provenanceRepo;
    private MockFlowFileRepository flowFileRepo;
    private CounterRepository counterRepository;
    private FlowFileEventRepository flowFileEventRepository;
    private final Relationship FAKE_RELATIONSHIP = new Relationship.Builder().name("FAKE").build();
    private static StandardResourceClaimManager resourceClaimManager;

    @After
    public void cleanup() {
        session.rollback();

        final File repoDir = new File("target/contentRepo");
        rmDir(repoDir);
    }

    private void rmDir(final File dir) {
        if (dir.isDirectory()) {
            final File[] children = dir.listFiles();
            if (children != null) {
                for (final File child : children) {
                    rmDir(child);
                }
            }
        }

        boolean removed = false;
        for (int i = 0; i < 100; i++) {
            removed = dir.delete();
            if (removed) {
                break;
            }
        }

        if (!removed && dir.exists()) {
            // we fail in this situation because it generally means that the StandardProcessSession did not
            // close the OutputStream.
            Assert.fail("Could not clean up content repo: " + dir + " could not be removed");
        }
    }

    @Before
    public void setup() throws IOException {
        resourceClaimManager = new StandardResourceClaimManager();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, StandardProcessSessionIT.class.getResource("/conf/nifi.properties").getFile());
        flowFileEventRepository = new RingBufferEventRepository(1);
        counterRepository = new StandardCounterRepository();
        provenanceRepo = new MockProvenanceRepository();

        final Connection connection = createConnection();

        final List<Connection> connList = new ArrayList<>();
        connList.add(connection);

        final ProcessGroup procGroup = Mockito.mock(ProcessGroup.class);
        when(procGroup.getIdentifier()).thenReturn("proc-group-identifier-1");

        connectable = Mockito.mock(Connectable.class);
        when(connectable.hasIncomingConnection()).thenReturn(true);
        when(connectable.getIncomingConnections()).thenReturn(connList);
        when(connectable.getProcessGroup()).thenReturn(procGroup);
        when(connectable.getIdentifier()).thenReturn("connectable-1");
        when(connectable.getConnectableType()).thenReturn(ConnectableType.INPUT_PORT);
        when(connectable.getComponentType()).thenReturn("Unit Test Component");

        Mockito.doAnswer(new Answer<Set<Connection>>() {
            @Override
            public Set<Connection> answer(final InvocationOnMock invocation) throws Throwable {
                final Object[] arguments = invocation.getArguments();
                final Relationship relationship = (Relationship) arguments[0];
                if (relationship == Relationship.SELF) {
                    return Collections.emptySet();
                } else if (relationship == FAKE_RELATIONSHIP || relationship.equals(FAKE_RELATIONSHIP)) {
                    return null;
                } else {
                    return new HashSet<>(connList);
                }
            }
        }).when(connectable).getConnections(Mockito.any(Relationship.class));

        when(connectable.getConnections()).thenReturn(new HashSet<>(connList));

        contentRepo = new MockContentRepository();
        contentRepo.initialize(new StandardResourceClaimManager());
        flowFileRepo = new MockFlowFileRepository(contentRepo);

        stateManager = new MockStateManager(connectable);
        stateManager.setIgnoreAnnotations(true);

        context = new StandardRepositoryContext(connectable, new AtomicLong(0L), contentRepo, flowFileRepo, flowFileEventRepository, counterRepository, provenanceRepo, stateManager);
        session = new StandardProcessSession(context, () -> false);
    }

    private Connection createConnection() {
        AtomicReference<FlowFileQueue> queueReference = new AtomicReference<>(flowFileQueue);
        Connection connection = createConnection(queueReference);
        flowFileQueue = queueReference.get();

        return connection;
    }

    private FlowFileQueue createFlowFileQueueSpy(Connection connection) {
        final FlowFileSwapManager swapManager = Mockito.mock(FlowFileSwapManager.class);
        final ProcessScheduler processScheduler = Mockito.mock(ProcessScheduler.class);

        final StandardFlowFileQueue actualQueue = new StandardFlowFileQueue("1", new NopConnectionEventListener(), flowFileRepo, provenanceRepo, null,
                processScheduler, swapManager, null, 10000, "0 sec", 0L, "0 B");
        return Mockito.spy(actualQueue);
    }

    @SuppressWarnings("unchecked")
    private Connection createConnection(AtomicReference<FlowFileQueue> flowFileQueueReference) {
        final Connection connection = Mockito.mock(Connection.class);

        FlowFileQueue flowFileQueueFromReference = flowFileQueueReference.get();

        final FlowFileQueue localFlowFileQueue;

        if (flowFileQueueFromReference == null) {
            localFlowFileQueue = createFlowFileQueueSpy(connection);
            flowFileQueueReference.set(localFlowFileQueue);
        } else {
            localFlowFileQueue = flowFileQueueFromReference;
        }

        when(connection.getFlowFileQueue()).thenReturn(localFlowFileQueue);

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                localFlowFileQueue.put((FlowFileRecord) invocation.getArguments()[0]);
                return null;
            }
        }).when(connection).enqueue(Mockito.any(FlowFileRecord.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                localFlowFileQueue.putAll((Collection<FlowFileRecord>) invocation.getArguments()[0]);
                return null;
            }
        }).when(connection).enqueue(Mockito.any(Collection.class));

        final Connectable dest = Mockito.mock(Connectable.class);
        when(connection.getDestination()).thenReturn(dest);
        when(connection.getSource()).thenReturn(dest);

        Mockito.doAnswer(new Answer<FlowFile>() {
            @Override
            public FlowFile answer(InvocationOnMock invocation) throws Throwable {
                return localFlowFileQueue.poll(invocation.getArgument(0));
            }
        }).when(connection).poll(any(Set.class));

        Mockito.doAnswer(new Answer<List<FlowFileRecord>>() {
            @Override
            public List<FlowFileRecord> answer(InvocationOnMock invocation) throws Throwable {
                return localFlowFileQueue.poll(invocation.getArgument(0), invocation.getArgument(1));
            }
        }).when(connection).poll(any(FlowFileFilter.class), any(Set.class));

        Mockito.when(connection.getIdentifier()).thenReturn("conn-uuid");
        return connection;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRoundRobinOnSessionGetNoArgs() {
        final List<Connection> connList = new ArrayList<>();
        final Connection conn1 = createConnection();
        final Connection conn2 = createConnection();
        connList.add(conn1);
        connList.add(conn2);

        final StandardFlowFileRecord.Builder flowFileRecordBuilder = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis());

        flowFileQueue.put(flowFileRecordBuilder.build());
        flowFileQueue.put(flowFileRecordBuilder.id(1001).build());

        when(connectable.getIncomingConnections()).thenReturn(connList);

        session.get();
        session.get();

        verify(conn1, times(1)).poll(any(Set.class));
        verify(conn2, times(1)).poll(any(Set.class));
    }

    @Test
    public void testFlowFileHandlingExceptionThrownIfMigratingChildNotParent() {
        final StandardFlowFileRecord.Builder flowFileRecordBuilder = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis());

        flowFileQueue.put(flowFileRecordBuilder.build());

        FlowFile flowFile = session.get();
        assertNotNull(flowFile);

        final List<FlowFile> children = new ArrayList<>();
        for (int i=0; i < 3; i++) {
            FlowFile child = session.create(flowFile);
            children.add(child);
        }

        final ProcessSession secondSession = new StandardProcessSession(context, () -> false);
        try {
            session.migrate(secondSession, children);
            Assert.fail("Expected a FlowFileHandlingException to be thrown because a child FlowFile was migrated while its parent was not");
        } catch (final FlowFileHandlingException expected) {
        }

        try {
            session.migrate(secondSession, Collections.singletonList(flowFile));
            Assert.fail("Expected a FlowFileHandlingException to be thrown because parent was forked and then migrated without children");
        } catch (final FlowFileHandlingException expected) {
        }

        try {
            session.migrate(secondSession, Arrays.asList(flowFile, children.get(0), children.get(1)));
            Assert.fail("Expected a FlowFileHandlingException to be thrown because parent was forked and then migrated without children");
        } catch (final FlowFileHandlingException expected) {
        }

        // Should succeed when migrating all FlowFiles.
        final List<FlowFile> allFlowFiles = new ArrayList<>();
        allFlowFiles.add(flowFile);
        allFlowFiles.addAll(children);
        session.migrate(secondSession, allFlowFiles);
        session.commit();

        final Relationship relationship = new Relationship.Builder().name("A").build();
        secondSession.transfer(allFlowFiles, relationship);
        secondSession.commit();
    }

    @Test
    public void testCloneForkChildMigrateCommit() throws IOException {
        final StandardFlowFileRecord.Builder flowFileRecordBuilder = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis());

        flowFileQueue.put(flowFileRecordBuilder.build());

        FlowFile flowFile = session.get();
        assertNotNull(flowFile);

        final ProcessSession secondSession = new StandardProcessSession(context, () -> false);

        FlowFile clone = session.clone(flowFile);
        session.migrate(secondSession, Collections.singletonList(clone));

        final List<FlowFile> children = new ArrayList<>();
        for (int i=0; i < 3; i++) {
            FlowFile child = secondSession.create(clone);
            children.add(child);
        }

        secondSession.transfer(children, Relationship.ANONYMOUS);
        secondSession.remove(clone);
        secondSession.commit();

        session.remove(flowFile);
        session.commit();

        final List<ProvenanceEventRecord> provEvents = provenanceRepo.getEvents(0L, 1000);
        assertEquals(3, provEvents.size());

        final Map<ProvenanceEventType, List<ProvenanceEventRecord>> eventsByType = provEvents.stream().collect(Collectors.groupingBy(ProvenanceEventRecord::getEventType));
        assertEquals(1, eventsByType.get(ProvenanceEventType.CLONE).size());
        assertEquals(1, eventsByType.get(ProvenanceEventType.DROP).size());
        assertEquals(1, eventsByType.get(ProvenanceEventType.FORK).size());

        final ProvenanceEventRecord fork = eventsByType.get(ProvenanceEventType.FORK).get(0);
        assertEquals(clone.getAttribute(CoreAttributes.UUID.key()), fork.getFlowFileUuid());
        assertEquals(Collections.singletonList(clone.getAttribute(CoreAttributes.UUID.key())), fork.getParentUuids());

        final Set<String> childUuids = children.stream().map(ff -> ff.getAttribute(CoreAttributes.UUID.key())).collect(Collectors.toSet());
        assertEquals(childUuids, new HashSet<>(fork.getChildUuids()));
    }

    @Test
    public void testCheckpointOnSessionDoesNotInteractWithFlowFile() {
        final Relationship relationship = new Relationship.Builder().name("A").build();

        session.adjustCounter("a", 1, false);
        session.adjustCounter("a", 1, true);
        session.checkpoint();

        session.adjustCounter("a", 1, true);
        session.adjustCounter("a", 2, false);
        session.adjustCounter("b", 3, false);
        session.checkpoint();

        assertEquals(2, counterRepository.getCounters().size());
        session.commit();

        final List<Counter> counters = counterRepository.getCounters();
        assertEquals(4, counters.size());

        int aCounters = 0;
        int bCounters = 0;
        for (final Counter counter : counters) {
            switch (counter.getName()) {
                case "a":
                    assertEquals(5, counter.getValue());
                    aCounters++;
                    break;
                case "b":
                    assertEquals(3, counter.getValue());
                    bCounters++;
                    break;
            }
        }

        assertEquals(2, aCounters);
        assertEquals(2, bCounters);
    }

    @Test
    public void testCheckpointMergesCounters() {
        final Relationship relationship = new Relationship.Builder().name("A").build();

        FlowFile flowFile = session.create();
        session.transfer(flowFile, relationship);
        session.adjustCounter("a", 1, false);
        session.checkpoint();

        flowFile = session.create();
        session.transfer(flowFile, relationship);
        session.adjustCounter("a", 1, false);
        session.adjustCounter("b", 3, false);
        session.checkpoint();

        assertEquals(0, counterRepository.getCounters().size());
        session.commit();

        // We should have 2 different counters with the name "a" and 2 different counters with the name "b" -
        // one for the "All Instances" context and one for the individual instance's context.
        final List<Counter> counters = counterRepository.getCounters();
        assertEquals(4, counters.size());

        int aCounters = 0;
        int bCounters = 0;
        for (final Counter counter : counters) {
            switch (counter.getName()) {
                case "a":
                    assertEquals(2, counter.getValue());
                    aCounters++;
                    break;
                case "b":
                    assertEquals(3, counter.getValue());
                    bCounters++;
                    break;
            }
        }

        assertEquals(2, aCounters);
        assertEquals(2, bCounters);
    }

    @Test
    public void testCombineCounters() {
        final Relationship relationship = new Relationship.Builder().name("A").build();

        FlowFile flowFile = session.create();
        session.transfer(flowFile, relationship);
        session.adjustCounter("a", 1, false);
        session.adjustCounter("b", 3, false);
        session.adjustCounter("a", 3, true);
        session.adjustCounter("b", 5, true);
        session.checkpoint();

        flowFile = session.create();
        session.transfer(flowFile, relationship);
        session.adjustCounter("a", 1, true);
        session.adjustCounter("b", 2, true);
        session.commit();

        context.getFlowFileEventRepository().reportTransferEvents(10L).getReportEntries().forEach((k, v) -> {
            v.getCounters().forEach((key, value) -> {
                if (key.equals("a")) {
                    assertEquals(5L, (long) value);
                }

                if (key.equals("b")) {
                    assertEquals(10L, (long) value);
                }
            });
        });
    }

    @Test
    public void testReadCountCorrectWhenSkippingWithReadCallback() throws IOException {
        final byte[] content = "This and that and the other.".getBytes(StandardCharsets.UTF_8);

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(contentRepo.create(content))
            .size(content.length)
            .build();

        flowFileQueue.put(flowFileRecord);

        FlowFile flowFile = session.get();
        session.read(flowFile, in -> {
            assertEquals('T', (char) in.read());
            in.mark(10);
            assertEquals(5, in.skip(5L));
            assertEquals('n', (char) in.read());
            in.reset();
        });

        session.transfer(flowFile);
        session.commit();

        final RepositoryStatusReport report = flowFileEventRepository.reportTransferEvents(0L);
        final long bytesRead = report.getReportEntry("connectable-1").getBytesRead();
        assertEquals(1, bytesRead);
    }

    @Test
    public void testReadCountCorrectWhenSkippingWithReadInputStream() throws IOException {
        final byte[] content = "This and that and the other.".getBytes(StandardCharsets.UTF_8);

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(contentRepo.create(content))
            .size(content.length)
            .build();

        flowFileQueue.put(flowFileRecord);

        FlowFile flowFile = session.get();
        try (InputStream in = session.read(flowFile)) {
            assertEquals('T', (char) in.read());
            in.mark(10);
            assertEquals(5, in.skip(5L));
            assertEquals('n', (char) in.read());
            in.reset();
        }

        session.transfer(flowFile);
        session.commit();

        final RepositoryStatusReport report = flowFileEventRepository.reportTransferEvents(0L);
        final long bytesRead = report.getReportEntry("connectable-1").getBytesRead();
        assertEquals(1, bytesRead);
    }

    public void testCheckpointMergesMaps() {
        for (int i=0; i < 2; i++) {
            final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .id(i)
                .entryDate(System.currentTimeMillis())
                .size(0L)
                .build();

            flowFileQueue.put(flowFileRecord);
        }

        final Relationship relationship = new Relationship.Builder().name("A").build();

        for (int i=0; i < 2; i++) {
            FlowFile ff1 = session.get();
            assertNotNull(ff1);
            session.transfer(ff1, relationship);
            session.adjustCounter("counter", 1, false);
            session.adjustCounter("counter", 1, true);
            session.checkpoint();
        }

        session.commit();

        final RepositoryStatusReport report = flowFileEventRepository.reportTransferEvents(0L);
        final FlowFileEvent queueFlowFileEvent = report.getReportEntry("conn-uuid");
        assertNotNull(queueFlowFileEvent);
        assertEquals(2, queueFlowFileEvent.getFlowFilesOut());
        assertEquals(0L, queueFlowFileEvent.getContentSizeOut());

        final FlowFileEvent componentFlowFileEvent = report.getReportEntry("connectable-1");
        final Map<String, Long> counters = componentFlowFileEvent.getCounters();
        assertNotNull(counters);
        assertEquals(1, counters.size());
        assertTrue(counters.containsKey("counter"));
        assertEquals(4L, counters.get("counter").longValue()); // increment twice for each FlowFile, once immediate, once not.
    }

    @Test
    public void testHandlingOfMultipleFlowFilesWithSameId() {
        // Add two FlowFiles with the same ID
        for (int i=0; i < 2; i++) {
            final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .id(1000L)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .size(0L)
                .build();

            flowFileQueue.put(flowFileRecord);
        }

        final Relationship relationship = new Relationship.Builder().name("A").build();

        FlowFile ff1 = session.get();
        assertNotNull(ff1);

        session.transfer(ff1, relationship);

        try {
            session.get();
            Assert.fail("Should not have been able to poll second FlowFile with same ID");
        } catch (final FlowFileAccessException e) {
            // Expected
        }
    }


    @Test
    public void testUpdateFlowFileRepoFailsOnSessionCommit() throws IOException {
        final ContentClaim contentClaim = contentRepo.create("original".getBytes());

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .size(8L)
            .contentClaim(contentClaim)
            .build();

        flowFileQueue.put(flowFileRecord);

        final Relationship relationship = new Relationship.Builder().name("A").build();

        FlowFile ff1 = session.get();
        assertNotNull(ff1);

        // Fork a child FlowFile.
        final FlowFile child = session.create(flowFileRecord);
        final FlowFile updated = session.write(flowFileRecord, out -> out.write("update".getBytes()));
        final ContentClaim updatedContentClaim = ((FlowFileRecord) updated).getContentClaim();

        session.remove(updated);
        final FlowFile updatedChild = session.write(child, out -> out.write("hello".getBytes(StandardCharsets.UTF_8)));
        session.transfer(updatedChild, relationship);

        final ContentClaim childContentClaim = ((FlowFileRecord) updatedChild).getContentClaim();

        flowFileRepo.setFailOnUpdate(true);

        assertEquals(1, contentRepo.getClaimantCount(contentClaim));

        // these will be the same content claim due to how the StandardProcessSession adds multiple FlowFiles' contents to a single claim.
        assertSame(updatedContentClaim, childContentClaim);
        assertEquals(2, contentRepo.getClaimantCount(childContentClaim));

        try {
            session.commit();
            Assert.fail("Expected session commit to fail");
        } catch (final ProcessException pe) {
            // Expected
        }

        // Ensure that if we fail to update teh flowfile repo, that the claimant count of the 'original' flowfile, which was removed, does not get decremented.
        assertEquals(1, contentRepo.getClaimantCount(contentClaim));
        assertEquals(0, contentRepo.getClaimantCount(updatedContentClaim)); // temporary claim should be cleaned up.
        assertEquals(0, contentRepo.getClaimantCount(childContentClaim)); // temporary claim should be cleaned up.

        assertEquals(1, flowFileQueue.size().getObjectCount());
        assertEquals(8L, flowFileQueue.size().getByteCount());
    }

    @Test
    public void testCloneOriginalDataSmaller() throws IOException {
        final byte[] originalContent = "hello".getBytes();
        final byte[] replacementContent = "NEW DATA".getBytes();

        final Connection conn1 = createConnection();
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(contentRepo.create(originalContent))
            .size(originalContent.length)
            .build();

        flowFileQueue.put(flowFileRecord);

        when(connectable.getIncomingConnections()).thenReturn(Collections.singletonList(conn1));

        final FlowFile input = session.get();
        assertEquals(originalContent.length, input.getSize());

        final FlowFile modified = session.write(input, (in, out) -> out.write(replacementContent));
        assertEquals(replacementContent.length, modified.getSize());

        // Clone 'input', not 'modified' because we want to ensure that we use the outdated reference to ensure
        // that the framework uses the most current reference.
        final FlowFile clone = session.clone(input);
        assertEquals(replacementContent.length, clone.getSize());

        final byte[] buffer = new byte[replacementContent.length];
        try (final InputStream in = session.read(clone)) {
            StreamUtils.fillBuffer(in, buffer);
        }

        assertArrayEquals(replacementContent, buffer);
    }

    @Test
    public void testEmbeddedReads() {
        FlowFile ff1 = session.write(session.create(), out -> out.write(new byte[] {'A', 'B'}));
        FlowFile ff2 = session.write(session.create(), out -> out.write('C'));

        session.read(ff1, in1 -> {
            int a = in1.read();
            assertEquals('A', a);

            session.read(ff2, in2 -> {
                int c = in2.read();
                assertEquals('C', c);
            });

            int b = in1.read();
            assertEquals('B', b);
        });
    }

    @Test
    public void testSequentialReads() throws IOException {
        FlowFile ff1 = session.write(session.create(), out -> out.write(new byte[] {'A', 'B'}));
        FlowFile ff2 = session.write(session.create(), out -> out.write('C'));

        final byte[] buff1 = new byte[2];
        try (final InputStream in = session.read(ff1)) {
            StreamUtils.fillBuffer(in, buff1);
        }

        final byte[] buff2 = new byte[1];
        try (final InputStream in = session.read(ff2)) {
            StreamUtils.fillBuffer(in, buff2);
        }

        Assert.assertArrayEquals(new byte[] {'A', 'B'}, buff1);
        Assert.assertArrayEquals(new byte[] {'C'}, buff2);
    }

    @Test
    public void testCloneOriginalDataLarger() throws IOException {
        final byte[] originalContent = "hello there 12345".getBytes();
        final byte[] replacementContent = "NEW DATA".getBytes();

        final Connection conn1 = createConnection();
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(contentRepo.create(originalContent))
            .size(originalContent.length)
            .build();

        flowFileQueue.put(flowFileRecord);

        when(connectable.getIncomingConnections()).thenReturn(Collections.singletonList(conn1));

        final FlowFile input = session.get();
        assertEquals(originalContent.length, input.getSize());

        final FlowFile modified = session.write(input, (in, out) -> out.write(replacementContent));
        assertEquals(replacementContent.length, modified.getSize());

        // Clone 'input', not 'modified' because we want to ensure that we use the outdated reference to ensure
        // that the framework uses the most current reference.
        final FlowFile clone = session.clone(input);
        assertEquals(replacementContent.length, clone.getSize());

        final byte[] buffer = new byte[replacementContent.length];
        try (final InputStream in = session.read(clone)) {
            StreamUtils.fillBuffer(in, buffer);
        }

        assertArrayEquals(replacementContent, buffer);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRoundRobinOnSessionGetWithCount() {
        final List<Connection> connList = new ArrayList<>();
        final Connection conn1 = createConnection();
        final Connection conn2 = createConnection();
        connList.add(conn1);
        connList.add(conn2);


        final StandardFlowFileRecord.Builder flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis());

        flowFileQueue.put(flowFileRecord.build());
        flowFileQueue.put(flowFileRecord.id(1001).build());

        when(connectable.getIncomingConnections()).thenReturn(connList);

        session.get(1);
        session.get(1);

        verify(conn1, times(1)).poll(any(FlowFileFilter.class), any(Set.class));
        verify(conn2, times(1)).poll(any(FlowFileFilter.class), any(Set.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRoundRobinAcrossConnectionsOnSessionGetWithCount() {
        final AtomicReference<FlowFileQueue> queue1Reference = new AtomicReference<>();
        final AtomicReference<FlowFileQueue> queue2Reference = new AtomicReference<>();

        final List<Connection> connList = new ArrayList<>();
        final Connection conn1 = createConnection(queue1Reference);
        final Connection conn2 = createConnection(queue2Reference);
        connList.add(conn1);
        connList.add(conn2);

        final FlowFileQueue queue2 = queue2Reference.get();

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .id(1000L)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();

        queue2.put(flowFileRecord);

        when(connectable.getIncomingConnections()).thenReturn(connList);

        List<FlowFile> result = session.get(2);

        assertEquals(1, result.size());

        verify(conn1, times(1)).poll(any(FlowFileFilter.class), any(Set.class));
        verify(conn2, times(1)).poll(any(FlowFileFilter.class), any(Set.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRoundRobinOnSessionGetWithFilter() {
        final List<Connection> connList = new ArrayList<>();
        final Connection conn1 = createConnection();
        final Connection conn2 = createConnection();
        connList.add(conn1);
        connList.add(conn2);

        final StandardFlowFileRecord.Builder flowFileRecordBuilder = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis());

        flowFileQueue.put(flowFileRecordBuilder.build());
        flowFileQueue.put(flowFileRecordBuilder.id(10001L).build());

        when(connectable.getIncomingConnections()).thenReturn(connList);

        final FlowFileFilter filter = ff -> FlowFileFilterResult.ACCEPT_AND_TERMINATE;

        session.get(filter);
        session.get(filter);

        verify(conn1, times(1)).poll(any(FlowFileFilter.class), any(Set.class));
        verify(conn2, times(1)).poll(any(FlowFileFilter.class), any(Set.class));
    }

    @Test
    public void testAppendToChildThrowsIOExceptionThenRemove() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .id(1000L)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile original = session.get();
        assertNotNull(original);

        FlowFile child = session.create(original);
        child = session.append(child, out -> out.write("hello".getBytes()));

        // Force an IOException. This will decrement out claim count for the resource claim.
        try {
            child = session.append(child, out -> {
                throw new IOException();
            });
            Assert.fail("append() callback threw IOException but it was not wrapped in ProcessException");
        } catch (final ProcessException pe) {
            // expected
        }

        session.remove(child);
        session.transfer(original);
        session.commit();

        final int numClaims = contentRepo.getExistingClaims().size();
        assertEquals(0, numClaims);
    }

    @Test
    public void testWriteForChildThrowsIOExceptionThenRemove() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .id(1000L)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile original = session.get();
        assertNotNull(original);

        FlowFile child = session.create(original);
        // Force an IOException. This will decrement out claim count for the resource claim.
        try {
            child = session.write(child, out -> out.write("hello".getBytes()));

            child = session.write(child, out -> {
                throw new IOException();
            });
            Assert.fail("write() callback threw IOException but it was not wrapped in ProcessException");
        } catch (final ProcessException pe) {
            // expected
        }

        session.remove(child);
        session.transfer(original);
        session.commit();

        final int numClaims = contentRepo.getExistingClaims().size();
        assertEquals(0, numClaims);
    }

    @Test
    public void testModifyContentWithStreamCallbackHasCorrectSize() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile original = session.get();
        assertNotNull(original);

        FlowFile child = session.write(original, (in, out) -> out.write("hello".getBytes()));
        session.transfer(child);
        session.commit();

        final FlowFileRecord onQueue = flowFileQueue.poll(Collections.emptySet());
        assertEquals(5, onQueue.getSize());
    }

    @Test
    public void testModifyContentWithOutputStreamCallbackHasCorrectSize() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile original = session.get();
        assertNotNull(original);

        FlowFile child = session.write(original, out -> out.write("hello".getBytes()));
        session.transfer(child);
        session.commit();

        final FlowFileRecord onQueue = flowFileQueue.poll(Collections.emptySet());
        assertEquals(5, onQueue.getSize());
    }

    @Test
    public void testModifyContentWithAppendHasCorrectSize() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1000L)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile original = session.get();
        assertNotNull(original);

        FlowFile child = session.append(original, out -> out.write("hello".getBytes()));
        session.transfer(child);
        session.commit();

        final FlowFileRecord onQueue = flowFileQueue.poll(Collections.emptySet());
        assertEquals(5, onQueue.getSize());
    }



    @Test
    public void testModifyContentThenRollback() throws IOException {
        assertEquals(0, contentRepo.getExistingClaims().size());

        final ContentClaim claim = contentRepo.create(false);
        assertEquals(1, contentRepo.getExistingClaims().size());

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .contentClaim(claim)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);

        FlowFile flowFile = session.get();
        assertNotNull(flowFile);
        flowFile = session.putAttribute(flowFile, "filename", "1.txt");
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
            }
        });
        session.transfer(flowFile);
        session.commit();
        assertEquals(1, contentRepo.getExistingClaims().size());

        flowFile = session.get();
        assertNotNull(flowFile);
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
            }
        });
        session.transfer(flowFile);
        session.commit();

        assertEquals(1, contentRepo.getExistingClaims().size());

        flowFile = session.get();
        assertNotNull(flowFile);
        session.remove(flowFile);
        session.rollback();
        assertEquals(1, contentRepo.getExistingClaims().size());

        flowFile = session.get();
        assertNotNull(flowFile);
        session.remove(flowFile);
        session.commit();
        assertEquals(0, contentRepo.getExistingClaims().size());
    }

    private void assertDisabled(final OutputStream outputStream) {
        try {
            outputStream.write(new byte[0]);
            Assert.fail("Expected OutputStream to be disabled; was able to call write(byte[])");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            outputStream.write(0);
            Assert.fail("Expected OutputStream to be disabled; was able to call write(int)");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            outputStream.write(new byte[0], 0, 0);
            Assert.fail("Expected OutputStream to be disabled; was able to call write(byte[], int, int)");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
    }

    private void assertDisabled(final InputStream inputStream) {
        try {
            inputStream.read();
            Assert.fail("Expected InputStream to be disabled; was able to call read()");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.read(new byte[0]);
            Assert.fail("Expected InputStream to be disabled; was able to call read(byte[])");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.read(new byte[0], 0, 0);
            Assert.fail("Expected InputStream to be disabled; was able to call read(byte[], int, int)");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.reset();
            Assert.fail("Expected InputStream to be disabled; was able to call reset()");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.skip(1L);
            Assert.fail("Expected InputStream to be disabled; was able to call skip(long)");
        } catch (final Exception ex) {
            assertEquals(FlowFileAccessException.class, ex.getClass());
        }
    }

    @Test
    public void testAppendAfterSessionClosesStream() throws IOException {
        final ContentClaim claim = contentRepo.create(false);
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .contentClaim(claim)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile flowFile = session.get();
        assertNotNull(flowFile);
        final AtomicReference<OutputStream> outputStreamHolder = new AtomicReference<>(null);
        flowFile = session.append(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream outputStream) throws IOException {
                outputStreamHolder.set(outputStream);
            }
        });
        assertDisabled(outputStreamHolder.get());
    }

    @Test
    public void testExportTo() throws IOException {
        final ContentClaim claim = contentRepo.create(false);
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .contentClaim(claim)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile flowFile = session.get();
        assertNotNull(flowFile);

        flowFile = session.append(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("Hello World".getBytes());
            }
        });

        // should be OK
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        session.exportTo(flowFile, os);
        assertEquals("Hello World", new String(os.toByteArray()));
        os.close();

        // should throw ProcessException because of IOException (from processor code)
        FileOutputStream mock = Mockito.mock(FileOutputStream.class);
        doThrow(new IOException()).when(mock).write((byte[]) notNull(), any(Integer.class), any(Integer.class));
        try {
            session.exportTo(flowFile, mock);
            Assert.fail("Expected ProcessException");
        } catch (ProcessException e) {
        }
    }

    @Test
    public void testReadAfterSessionClosesStream() throws IOException {
        final ContentClaim claim = contentRepo.create(false);
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .contentClaim(claim)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);
        final FlowFile flowFile = session.get();
        assertNotNull(flowFile);
        final AtomicReference<InputStream> inputStreamHolder = new AtomicReference<>(null);
        session.read(flowFile, true, new InputStreamCallback() {
            @Override
            public void process(final InputStream inputStream) throws IOException {
                inputStreamHolder.set(inputStream);
            }
        });
        assertDisabled(inputStreamHolder.get());
    }

    @Test
    public void testStreamAfterSessionClosesStream() throws IOException {
        final ContentClaim claim = contentRepo.create(false);
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .contentClaim(claim)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile flowFile = session.get();
        assertNotNull(flowFile);
        final AtomicReference<InputStream> inputStreamHolder = new AtomicReference<>(null);
        final AtomicReference<OutputStream> outputStreamHolder = new AtomicReference<>(null);
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(final InputStream input, final OutputStream output) throws IOException {
                inputStreamHolder.set(input);
                outputStreamHolder.set(output);
            }
        });
        assertDisabled(inputStreamHolder.get());
        assertDisabled(outputStreamHolder.get());
    }

    @Test
    public void testWriteAfterSessionClosesStream() throws IOException {
        final ContentClaim claim = contentRepo.create(false);
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .contentClaim(claim)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);
        FlowFile flowFile = session.get();
        assertNotNull(flowFile);
        final AtomicReference<OutputStream> outputStreamHolder = new AtomicReference<>(null);
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                outputStreamHolder.set(out);
            }
        });
        assertDisabled(outputStreamHolder.get());
    }

    @Test
    public void testCreateThenRollbackRemovesContent() throws IOException {

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();
        flowFileQueue.put(flowFileRecord);

        final StreamCallback nop = new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
            }
        };

        session.create();
        FlowFile flowFile = session.create(flowFileRecord);
        flowFile = session.write(flowFile, nop);

        FlowFile flowFile2 = session.create(flowFileRecord);
        flowFile2 = session.write(flowFile2, nop);

        session.write(flowFile2, nop);

        final FlowFile flowFile3 = session.create();
        session.write(flowFile3, nop);

        session.rollback();
        assertEquals(4, contentRepo.getClaimsRemoved());
    }

    @Test
    public void testForksNotEmittedIfFilesDeleted() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord);

        final FlowFile orig = session.get();
        final FlowFile newFlowFile = session.create(orig);
        session.remove(newFlowFile);
        session.commit();

        assertEquals(0, provenanceRepo.getEvents(0L, 100000).size());
    }

    @Test
    public void testProvenanceEventsEmittedForForkIfNotRemoved() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord);

        final FlowFile orig = session.get();
        final FlowFile newFlowFile = session.create(orig);
        session.transfer(newFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        assertEquals(1, provenanceRepo.getEvents(0L, 100000).size()); // 1 event for both parents and children
    }

    @Test
    public void testProvenanceEventsEmittedForRemove() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord);

        final FlowFile orig = session.get();
        final FlowFile newFlowFile = session.create(orig);
        final FlowFile secondNewFlowFile = session.create(orig);
        session.remove(newFlowFile);
        session.transfer(secondNewFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        assertEquals(1, provenanceRepo.getEvents(0L, 100000).size());
    }

    @Test
    public void testProvenanceEventsHaveDurationFromSession() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord);

        final FlowFile orig = session.get();
        final FlowFile newFlowFile = session.create(orig);
        session.getProvenanceReporter().fork(orig, Collections.singletonList(newFlowFile), 0L);
        session.getProvenanceReporter().fetch(newFlowFile, "nowhere://");
        session.getProvenanceReporter().send(newFlowFile, "nowhere://");
        session.transfer(newFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 100000);
        assertNotNull(events);
        assertEquals(3, events.size()); // FETCH, SEND, and FORK
        events.forEach((event) -> assertTrue(event.getEventDuration() > -1));
    }

    @Test
    public void testUuidAttributeCannotBeUpdated() {
        String originalUuid = "11111111-1111-1111-1111-111111111111";
        final FlowFileRecord flowFileRecord1 = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", originalUuid)
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord1);

        FlowFile flowFile = session.get();
        assertNotNull(flowFile);

        final String uuid = CoreAttributes.UUID.key();
        final String newUuid = "22222222-2222-2222-2222-222222222222";
        flowFile = session.putAttribute(flowFile, uuid, newUuid);
        assertEquals(originalUuid, flowFile.getAttribute(uuid));

        final Map<String, String> uuidMap = new HashMap<>(1);
        uuidMap.put(uuid, newUuid);

        flowFile = session.putAllAttributes(flowFile, uuidMap);
        assertEquals(originalUuid, flowFile.getAttribute(uuid));

        flowFile = session.removeAllAttributes(flowFile, Pattern.compile("uuid"));
        assertEquals(originalUuid, flowFile.getAttribute(uuid));

        flowFile = session.removeAllAttributes(flowFile, Collections.singleton(uuid));
        assertEquals(originalUuid, flowFile.getAttribute(uuid));

        flowFile = session.removeAttribute(flowFile, uuid);
        assertEquals(originalUuid, flowFile.getAttribute(uuid));

    }

    @Test
    public void testUpdateAttributesThenJoin() throws IOException {
        final FlowFileRecord flowFileRecord1 = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", "11111111-1111-1111-1111-111111111111")
                .entryDate(System.currentTimeMillis())
                .build();

        final FlowFileRecord flowFileRecord2 = new StandardFlowFileRecord.Builder()
                .id(2L)
                .addAttribute("uuid", "22222222-2222-2222-2222-222222222222")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord1);
        flowFileQueue.put(flowFileRecord2);

        FlowFile ff1 = session.get();
        FlowFile ff2 = session.get();

        ff1 = session.putAttribute(ff1, "index", "1");
        ff2 = session.putAttribute(ff2, "index", "2");

        final List<FlowFile> parents = new ArrayList<>(2);
        parents.add(ff1);
        parents.add(ff2);

        final FlowFile child = session.create(parents);

        final Relationship rel = new Relationship.Builder().name("A").build();

        session.transfer(ff1, rel);
        session.transfer(ff2, rel);
        session.transfer(child, rel);

        session.commit();

        final List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 1000);

        // We should have a JOIN and 2 ATTRIBUTE_MODIFIED's
        assertEquals(3, events.size());

        int joinCount = 0;
        int ff1UpdateCount = 0;
        int ff2UpdateCount = 0;

        for (final ProvenanceEventRecord event : events) {
            switch (event.getEventType()) {
                case JOIN:
                    assertEquals(child.getAttribute("uuid"), event.getFlowFileUuid());
                    joinCount++;
                    break;
                case ATTRIBUTES_MODIFIED:
                    if (event.getFlowFileUuid().equals(ff1.getAttribute("uuid"))) {
                        ff1UpdateCount++;
                    } else if (event.getFlowFileUuid().equals(ff2.getAttribute("uuid"))) {
                        ff2UpdateCount++;
                    } else {
                        Assert.fail("Got ATTRIBUTE_MODIFIED for wrong FlowFile: " + event.getFlowFileUuid());
                    }
                    break;
                default:
                    Assert.fail("Unexpected event type: " + event);
            }
        }

        assertEquals(1, joinCount);
        assertEquals(1, ff1UpdateCount);
        assertEquals(1, ff2UpdateCount);

        assertEquals(1, joinCount);
    }

    @Test
    public void testForkOneToOneReported() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord);

        // we have to increment the ID generator because we are creating a FlowFile without the FlowFile Repository's knowledge
        flowFileRepo.idGenerator.getAndIncrement();

        final FlowFile orig = session.get();
        final FlowFile newFlowFile = session.create(orig);
        session.transfer(newFlowFile, new Relationship.Builder().name("A").build());
        session.getProvenanceReporter().fork(newFlowFile, Collections.singleton(orig));
        session.remove(orig);
        session.commit();

        final List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 1000);
        assertEquals(2, events.size());

        final ProvenanceEventRecord firstRecord = events.get(0);
        final ProvenanceEventRecord secondRecord = events.get(1);

        assertEquals(ProvenanceEventType.FORK, firstRecord.getEventType());
        assertEquals(ProvenanceEventType.DROP, secondRecord.getEventType());
    }

    @Test
    public void testProcessExceptionThrownIfCallbackThrowsInOutputStreamCallback() {
        final FlowFile ff1 = session.create();

        final RuntimeException runtime = new RuntimeException();
        try {
            session.write(ff1, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    throw runtime;
                }
            });
            Assert.fail("Should have thrown RuntimeException");
        } catch (final RuntimeException re) {
            assertTrue(runtime == re);
        }

        final IOException ioe = new IOException();
        try {
            session.write(ff1, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    throw ioe;
                }
            });
            Assert.fail("Should have thrown ProcessException");
        } catch (final ProcessException pe) {
            assertTrue(ioe == pe.getCause());
        }

        final ProcessException pe = new ProcessException();
        try {
            session.write(ff1, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    throw pe;
                }
            });
            Assert.fail("Should have thrown ProcessException");
        } catch (final ProcessException pe2) {
            assertTrue(pe == pe2);
        }
    }

    @Test
    public void testProcessExceptionThrownIfCallbackThrowsInStreamCallback() {
        final FlowFile ff1 = session.create();

        final RuntimeException runtime = new RuntimeException();
        try {
            session.write(ff1, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    throw runtime;
                }
            });
            Assert.fail("Should have thrown RuntimeException");
        } catch (final RuntimeException re) {
            assertTrue(runtime == re);
        }

        final IOException ioe = new IOException();
        try {
            session.write(ff1, new StreamCallback() {
                @Override
                public void process(final InputStream in, OutputStream out) throws IOException {
                    throw ioe;
                }
            });
            Assert.fail("Should have thrown ProcessException");
        } catch (final ProcessException pe) {
            assertTrue(ioe == pe.getCause());
        }

        final ProcessException pe = new ProcessException();
        try {
            session.write(ff1, new StreamCallback() {
                @Override
                public void process(final InputStream in, OutputStream out) throws IOException {
                    throw pe;
                }
            });
            Assert.fail("Should have thrown ProcessException");
        } catch (final ProcessException pe2) {
            assertTrue(pe == pe2);
        }
    }

    @Test
    public void testMissingFlowFileExceptionThrownWhenUnableToReadData() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
                .size(1L)
                .build();
        flowFileQueue.put(flowFileRecord);

        // attempt to read the data.
        try {
            final FlowFile ff1 = session.get();

            session.read(ff1, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    in.read();
                }
            });
            Assert.fail("Expected MissingFlowFileException");
        } catch (final MissingFlowFileException mffe) {
        }
    }

    @Test
    public void testAppend() throws IOException {
        FlowFile ff = session.create();
        ff = session.append(ff, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("Hello".getBytes());
            }
        });

        // do not allow the content repo to be read from; this ensures that we are
        // not copying the data each time we call append but instead are actually appending to the output stream
        contentRepo.disableRead = true;
        ff = session.append(ff, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(", ".getBytes());
            }
        });

        ff = session.append(ff, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("World".getBytes());
            }
        });

        contentRepo.disableRead = false;
        final byte[] buff = new byte["Hello, World".getBytes().length];
        session.read(ff, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buff);
            }
        });

        assertEquals("Hello, World", new String(buff));
    }

    @Test
    public void testAppendToFlowFileWhereResourceClaimHasMultipleContentClaims() throws IOException {
        final Relationship relationship = new Relationship.Builder().name("A").build();

        FlowFile ffa = session.create();
        ffa = session.write(ffa, (out) -> out.write('A'));
        session.transfer(ffa, relationship);

        FlowFile ffb = session.create();
        ffb = session.write(ffb, (out) -> out.write('B'));
        session.transfer(ffb, relationship);
        session.commit();

        final ProcessSession newSession = new StandardProcessSession(context, () -> false);
        FlowFile toUpdate = newSession.get();
        newSession.append(toUpdate, out -> out.write('C'));

        // Read the content back and ensure that it is correct
        final byte[] buff;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            newSession.read(toUpdate, in -> StreamUtils.copy(in, baos));
            buff = baos.toByteArray();
        }

        final String output = new String(buff, StandardCharsets.UTF_8);
        assertEquals("AC", output);
        newSession.transfer(toUpdate);
        newSession.commit();
    }

    @Test
    public void testAppendDoesNotDecrementContentClaimIfNotNeeded() {
        FlowFile flowFile = session.create();

        session.append(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("hello".getBytes());
            }
        });

        final Set<ContentClaim> existingClaims = contentRepo.getExistingClaims();
        assertEquals(1, existingClaims.size());
        final ContentClaim claim = existingClaims.iterator().next();

        final int countAfterAppend = contentRepo.getClaimantCount(claim);
        assertEquals(1, countAfterAppend);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExpireDecrementsClaimsOnce() throws IOException {
        final ContentClaim contentClaim = contentRepo.create(false);

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .contentClaim(contentClaim)
                .build();

        Mockito.doAnswer(new Answer<List<FlowFileRecord>>() {
            int iterations = 0;

            @Override
            public List<FlowFileRecord> answer(InvocationOnMock invocation) throws Throwable {
                if (iterations++ == 0) {
                    final Set<FlowFileRecord> expired = invocation.getArgument(1);
                    expired.add(flowFileRecord);
                }

                return null;
            }
        }).when(flowFileQueue).poll(Mockito.any(FlowFileFilter.class), Mockito.any(Set.class));

        session.expireFlowFiles();
        session.commit(); // if the content claim count is decremented to less than 0, an exception will be thrown.

        assertEquals(1L, contentRepo.getClaimsRemoved());
    }

    @Test
    @Ignore
    public void testManyFilesOpened() throws IOException {

        StandardProcessSession[] standardProcessSessions = new StandardProcessSession[100000];
        for (int i = 0; i < 70000; i++) {
            standardProcessSessions[i] = new StandardProcessSession(context, () -> false);

            FlowFile flowFile = standardProcessSessions[i].create();
            final byte[] buff = new byte["Hello".getBytes().length];

            flowFile = standardProcessSessions[i].append(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write("Hello".getBytes());
                }
            });

            try {
                standardProcessSessions[i].read(flowFile, false, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        StreamUtils.fillBuffer(in, buff);
                    }
                });
            } catch (Exception e) {
                System.out.println("Failed at file:" + i);
                throw e;
            }
            if (i % 1000 == 0) {
                System.out.println("i:" + i);
            }
        }
    }

    @Test
    public void testMissingFlowFileExceptionThrownWhenUnableToReadDataStreamCallback() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
                .size(1L)
                .build();
        flowFileQueue.put(flowFileRecord);

        // attempt to read the data.
        try {
            final FlowFile ff1 = session.get();

            session.write(ff1, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    in.read();
                }
            });
            Assert.fail("Expected MissingFlowFileException");
        } catch (final MissingFlowFileException mffe) {
        }
    }

    @Test
    public void testContentNotFoundExceptionThrownWhenUnableToReadDataStreamCallbackOffsetTooLarge() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
            .build();
        flowFileQueue.put(flowFileRecord);

        FlowFile ff1 = session.get();
        ff1 = session.write(ff1, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
            }
        });
        session.transfer(ff1);
        session.commit();

        final FlowFileRecord flowFileRecord2 = new StandardFlowFileRecord.Builder()
            .id(2)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
            .contentClaimOffset(1000L)
            .size(1000L)
            .build();
        flowFileQueue.put(flowFileRecord2);

        // attempt to read the data.
        try {
            session.get();
            final FlowFile ff2 = session.get();
            session.write(ff2, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    in.read();
                }
            });
            Assert.fail("Expected ContentNotFoundException");
        } catch (final MissingFlowFileException mffe) {
        }
    }

    @Test
    public void testContentNotFoundExceptionThrownWhenUnableToReadDataOffsetTooLarge() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(1)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
            .build();

        flowFileQueue.put(flowFileRecord);

        FlowFile ff1 = session.get();
        ff1 = session.write(ff1, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
            }
        });
        session.transfer(ff1);
        session.commit();

        final FlowFileRecord flowFileRecord2 = new StandardFlowFileRecord.Builder()
            .id(2)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
            .contentClaimOffset(1000L).size(1L).build();
        flowFileQueue.put(flowFileRecord2);

        // attempt to read the data.
        try {
            session.get();
            final FlowFile ff2 = session.get();
            session.read(ff2, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    in.read();
                }
            });
            Assert.fail("Expected MissingFlowFileException");
        } catch (final MissingFlowFileException mffe) {
        }
    }

    @Test
    public void testProcessExceptionThrownIfCallbackThrowsInInputStreamCallback() {
        final FlowFile ff1 = session.create();

        final RuntimeException runtime = new RuntimeException();
        try {
            session.read(ff1, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    throw runtime;
                }
            });
            Assert.fail("Should have thrown RuntimeException");
        } catch (final RuntimeException re) {
            assertTrue(runtime == re);
        }

        final IOException ioe = new IOException();
        try {
            session.read(ff1, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    throw ioe;
                }
            });
            Assert.fail("Should have thrown ProcessException");
        } catch (final ProcessException pe) {
            assertTrue(ioe == pe.getCause());
        }

        final ProcessException pe = new ProcessException();
        try {
            session.read(ff1, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    throw pe;
                }
            });
            Assert.fail("Should have thrown ProcessException");
        } catch (final ProcessException pe2) {
            assertTrue(pe == pe2);
        }
    }

    @Test
    public void testCommitFailureRequeuesFlowFiles() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
                .contentClaimOffset(0L).size(0L).build();
        flowFileQueue.put(flowFileRecord);

        final FlowFile originalFlowFile = session.get();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertTrue(flowFileQueue.isUnacknowledgedFlowFile());

        final FlowFile modified = session.write(originalFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("Hello".getBytes());
            }
        });

        session.transfer(modified);

        // instruct flowfile repo to throw IOException on update
        flowFileRepo.setFailOnUpdate(true);

        try {
            session.commit();
            Assert.fail("Session commit completed, even though FlowFile Repo threw IOException");
        } catch (final ProcessException pe) {
            // expected behavior because FlowFile Repo will throw IOException
        }

        assertFalse(flowFileQueue.isActiveQueueEmpty());
        assertEquals(1, flowFileQueue.size().getObjectCount());
        assertFalse(flowFileQueue.isUnacknowledgedFlowFile());
    }

    @Test
    public void testRollbackAfterCheckpoint() {
        final StandardFlowFileRecord.Builder recordBuilder = new StandardFlowFileRecord.Builder()
            .id(1)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true, false), 0L))
            .contentClaimOffset(0L)
            .size(0L);

        final FlowFileRecord flowFileRecord = recordBuilder.build();
        flowFileQueue.put(flowFileRecord);

        final FlowFile originalFlowFile = session.get();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertTrue(flowFileQueue.isUnacknowledgedFlowFile());

        final FlowFile modified = session.write(originalFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("Hello".getBytes());
            }
        });

        session.transfer(modified);

        session.checkpoint();
        assertTrue(flowFileQueue.isActiveQueueEmpty());

        session.rollback();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertEquals(0, flowFileQueue.size().getObjectCount());
        assertFalse(flowFileQueue.isUnacknowledgedFlowFile());

        session.rollback();

        flowFileQueue.put(recordBuilder.id(2).build());
        assertFalse(flowFileQueue.isActiveQueueEmpty());

        final FlowFile originalRound2 = session.get();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertTrue(flowFileQueue.isUnacknowledgedFlowFile());

        final FlowFile modifiedRound2 = session.write(originalRound2, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("Hello".getBytes());
            }
        });

        session.transfer(modifiedRound2);

        session.checkpoint();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertTrue(flowFileQueue.isUnacknowledgedFlowFile());

        session.commit();

        // FlowFiles transferred back to queue
        assertEquals(2, flowFileQueue.size().getObjectCount());
        assertFalse(flowFileQueue.isUnacknowledgedFlowFile());
        assertFalse(flowFileQueue.isActiveQueueEmpty());
    }

    @Test
    public void testCreateEmitted() throws IOException {
        final FlowFile newFlowFile = session.create();
        session.transfer(newFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        final List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 10000);
        assertFalse(events.isEmpty());
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.CREATE, event.getEventType());
    }

    @Test
    public void testContentModifiedNotEmittedForCreate() throws IOException {
        FlowFile newFlowFile = session.create();
        newFlowFile = session.write(newFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
            }
        });
        session.transfer(newFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        final List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 10000);
        assertFalse(events.isEmpty());
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.CREATE, event.getEventType());
    }

    @Test
    public void testContentModifiedEmittedAndNotAttributesModified() throws IOException {
        final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", "000000000000-0000-0000-0000-00000000")
                .build();
        this.flowFileQueue.put(flowFile);

        FlowFile existingFlowFile = session.get();
        existingFlowFile = session.write(existingFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
            }
        });
        existingFlowFile = session.putAttribute(existingFlowFile, "attr", "a");
        session.transfer(existingFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        final List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 10000);
        assertFalse(events.isEmpty());
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.CONTENT_MODIFIED, event.getEventType());
    }

    @Test
    public void testGetWithCount() {
        for (int i = 0; i < 8; i++) {
            final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                    .id(i)
                    .addAttribute("uuid", "000000000000-0000-0000-0000-0000000" + i)
                    .build();
            this.flowFileQueue.put(flowFile);
        }

        final List<FlowFile> flowFiles = session.get(7);
        assertEquals(7, flowFiles.size());
    }

    @Test
    public void testBatchQueuedHaveSameQueuedTime() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (i == 99) {
                Thread.sleep(10);
            }

            final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                    .id(i)
                    .addAttribute("uuid", "000000000000-0000-0000-0000-0000000" + i)
                    .build();

            this.flowFileQueue.put(flowFile);
        }

        final List<FlowFile> flowFiles = session.get(100);

        // FlowFile Queued times should not match yet
        assertNotEquals("Queued times should not be equal.", flowFiles.get(0).getLastQueueDate(), flowFiles.get(99).getLastQueueDate());

        session.transfer(flowFiles, new Relationship.Builder().name("A").build());
        session.commit();

        final List<FlowFile> flowFilesUpdated = session.get(100);

        // FlowFile Queued times should match
        assertEquals("Queued times should be equal.", flowFilesUpdated.get(0).getLastQueueDate(), flowFilesUpdated.get(99).getLastQueueDate());
    }

    @Test
    public void testAttributesModifiedEmitted() throws IOException {
        final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", "000000000000-0000-0000-0000-00000000")
                .build();
        this.flowFileQueue.put(flowFile);

        FlowFile existingFlowFile = session.get();
        existingFlowFile = session.putAttribute(existingFlowFile, "attr", "a");
        session.transfer(existingFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        final List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 10000);
        assertFalse(events.isEmpty());
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.ATTRIBUTES_MODIFIED, event.getEventType());
    }

    @Test
    public void testReadFromInputStream() throws IOException {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write("hello, world".getBytes());
            }
        });

        try (InputStream in = session.read(flowFile)) {
            final byte[] buffer = new byte[12];
            StreamUtils.fillBuffer(in, buffer);
            assertEquals("hello, world", new String(buffer));
        }

        session.remove(flowFile);
        session.commit();
    }

    @Test
    public void testReadFromInputStreamWithoutClosingThenRemove() throws IOException {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write("hello, world".getBytes());
            }
        });

        InputStream in = session.read(flowFile);
        final byte[] buffer = new byte[12];
        StreamUtils.fillBuffer(in, buffer);
        assertEquals("hello, world", new String(buffer));

        try {
            session.remove(flowFile);
            Assert.fail("Was able to remove FlowFile while an InputStream is open for it");
        } catch (final IllegalStateException e) {
            // expected
        }

        in.close();

        session.remove(flowFile);

        session.commit(); // This should generate a WARN log message. We can't really test this in a unit test but can verify manually.
    }

    @Test
    public void testOpenMultipleInputStreamsToFlowFile() throws IOException {
        final ContentClaim claim = contentRepo.create(false);
        try (final OutputStream out = contentRepo.write(claim)) {
            out.write("hello, world".getBytes());
        }

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .contentClaim(claim)
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .size(12L)
                .build();
        flowFileQueue.put(flowFileRecord);

        final FlowFile flowFile = session.get();
        InputStream in = session.read(flowFile);
        final byte[] buffer = new byte[12];
        StreamUtils.fillBuffer(in, buffer);
        assertEquals("hello, world", new String(buffer));

        InputStream in2 = session.read(flowFile);
        StreamUtils.fillBuffer(in2, buffer);
        assertEquals("hello, world", new String(buffer));

        in.close();
        in2.close();
        session.remove(flowFile);
        session.commit();
    }

    @Test
    public void testWriteToOutputStream() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .size(12L)
            .build();

        flowFileQueue.put(flowFileRecord);

        FlowFile flowFile = session.get();
        try (final OutputStream out = session.write(flowFile)) {
            out.write("hello, world".getBytes());
        }

        // Call putAllAttributes, because this will return to us the most recent version
        // of the FlowFile. In a Processor, we wouldn't need this, but for testing purposes
        // we need it in order to get the Content Claim.
        flowFile = session.putAllAttributes(flowFile, Collections.emptyMap());
        assertEquals(12L, flowFile.getSize());

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        try (final InputStream in = session.read(flowFile)) {
            StreamUtils.fillBuffer(in, buffer);
        }

        assertEquals(new String(buffer), "hello, world");
    }

    @Test
    public void testWriteToOutputStreamWhileReading() throws IOException {
        final ContentClaim claim = contentRepo.create(false);
        try (final OutputStream out = contentRepo.write(claim)) {
            out.write("hello, world".getBytes());
        }

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .contentClaim(claim)
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .size(12L)
            .build();
        flowFileQueue.put(flowFileRecord);

        final FlowFile flowFile = session.get();
        InputStream in = session.read(flowFile);

        try {
            session.write(flowFile);
            Assert.fail("Was able to obtain an OutputStream for a FlowFile while also holding an InputStream for it");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            in.close();
        }

        // Should now be okay
        try (final OutputStream out = session.write(flowFile)) {

        }
    }

    @Test
    public void testReadFromInputStreamWhileWriting() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .size(12L)
            .build();
        flowFileQueue.put(flowFileRecord);

        final FlowFile flowFile = session.get();
        OutputStream out = session.write(flowFile);

        try {
            session.read(flowFile);
            Assert.fail("Was able to obtain an InputStream for a FlowFile while also holding an OutputStream for it");
        } catch (final IllegalStateException e) {
            // expected
        } finally {
            out.close();
        }

        // Should now be okay
        try (final InputStream in = session.read(flowFile)) {

        }
    }


    @Test
    public void testTransferUnknownRelationship() {
        final FlowFileRecord flowFileRecord1 = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", "11111111-1111-1111-1111-111111111111")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord1);

        FlowFile ff1 = session.get();
        ff1 = session.putAttribute(ff1, "index", "1");

        try {
            session.transfer(ff1, FAKE_RELATIONSHIP);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            final Collection<FlowFile> collection = new HashSet<>();
            collection.add(ff1);
            session.transfer(collection, FAKE_RELATIONSHIP);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testMigrateWithAppendableStream() throws IOException {
        FlowFile flowFile = session.create();
        flowFile = session.append(flowFile, out -> out.write("1".getBytes()));
        flowFile = session.append(flowFile, out -> out.write("2".getBytes()));

        final StandardProcessSession newSession = new StandardProcessSession(context, () -> false);

        assertTrue(session.isFlowFileKnown(flowFile));
        assertFalse(newSession.isFlowFileKnown(flowFile));

        session.migrate(newSession, Collections.singleton(flowFile));

        assertFalse(session.isFlowFileKnown(flowFile));
        assertTrue(newSession.isFlowFileKnown(flowFile));

        flowFile = newSession.append(flowFile, out -> out.write("3".getBytes()));

        final byte[] buff = new byte[3];
        try (final InputStream in = newSession.read(flowFile)) {
            StreamUtils.fillBuffer(in, buff, true);
            assertEquals(-1, in.read());
        }

        assertTrue(Arrays.equals(new byte[] {'1', '2', '3'}, buff));

        newSession.remove(flowFile);
        newSession.commit();
        session.commit();
    }

    @Test
    public void testNewFlowFileModifiedMultipleTimesHasTransientClaimsOnCommit() {
        FlowFile flowFile = session.create();
        for (int i = 0; i < 5; i++) {
            final byte[] content = String.valueOf(i).getBytes();
            flowFile = session.write(flowFile, out -> out.write(content));
        }

        session.transfer(flowFile, new Relationship.Builder().name("success").build());
        session.commit();

        final List<RepositoryRecord> repoUpdates = flowFileRepo.getUpdates();
        assertEquals(1, repoUpdates.size());

        // Should be 4 transient claims because it was written to 5 times. So 4 transient + 1 actual claim.
        final RepositoryRecord record = repoUpdates.get(0);
        assertEquals(RepositoryRecordType.CREATE, record.getType());
        final List<ContentClaim> transientClaims = record.getTransientClaims();
        assertEquals(4, transientClaims.size());
    }


    @Test
    public void testUpdateFlowFileModifiedMultipleTimesHasTransientClaimsOnCommit() {
        flowFileQueue.put(new MockFlowFile(1L));

        FlowFile flowFile = session.get();
        for (int i = 0; i < 5; i++) {
            final byte[] content = String.valueOf(i).getBytes();
            flowFile = session.write(flowFile, out -> out.write(content));
        }

        session.transfer(flowFile, new Relationship.Builder().name("success").build());
        session.commit();

        final List<RepositoryRecord> repoUpdates = flowFileRepo.getUpdates();
        assertEquals(1, repoUpdates.size());

        // Should be 4 transient claims because it was written to 5 times. So 4 transient + 1 actual claim.
        final RepositoryRecord record = repoUpdates.get(0);
        assertEquals(RepositoryRecordType.UPDATE, record.getType());
        final List<ContentClaim> transientClaims = record.getTransientClaims();
        assertEquals(4, transientClaims.size());
    }


    @Test
    public void testUpdateFlowFileModifiedMultipleTimesHasTransientClaimsOnRollback() {
        flowFileQueue.put(new MockFlowFile(1L));

        FlowFile flowFile = session.get();
        for (int i = 0; i < 5; i++) {
            final byte[] content = String.valueOf(i).getBytes();
            flowFile = session.write(flowFile, out -> out.write(content));
        }

        session.rollback();

        final List<RepositoryRecord> repoUpdates = flowFileRepo.getUpdates();
        assertEquals(1, repoUpdates.size());

        // Should be 5 transient claims because it was written to 5 times and then rolled back so all
        // content claims are 'transient'.
        final RepositoryRecord record = repoUpdates.get(0);
        assertEquals(RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS, record.getType());
        final List<ContentClaim> transientClaims = record.getTransientClaims();
        assertEquals(5, transientClaims.size());
    }

    @Test
    public void testMultipleReadCounts() throws IOException {
        flowFileQueue.put(new MockFlowFile(1L));

        FlowFile flowFile = session.get();

        final List<InputStream> streams = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            streams.add(session.read(flowFile));
        }

        for (int i = 0; i < 3; i++) {
            try {
                flowFile = session.putAttribute(flowFile, "counter", String.valueOf(i));
                Assert.fail("Was able to put attribute while reading");
            } catch (final IllegalStateException ise) {
                // expected
            }

            streams.get(i).close();
        }

        flowFile = session.putAttribute(flowFile, "counter", "4");
    }

    @Test
    public void testStateStoredOnCommit() throws IOException {
        session.commit();
        stateManager.assertStateNotSet();

        session.setState(Collections.singletonMap("abc", "123"), Scope.LOCAL);
        stateManager.assertStateNotSet();

        session.commit();
        stateManager.assertStateEquals("abc", "123", Scope.LOCAL);
        stateManager.assertStateNotSet(Scope.CLUSTER);

        stateManager.reset();

        session.setState(Collections.singletonMap("abc", "123"), Scope.CLUSTER);
        stateManager.assertStateNotSet();

        session.commit();
        stateManager.assertStateEquals("abc", "123", Scope.CLUSTER);
        stateManager.assertStateNotSet(Scope.LOCAL);
    }

    @Test
    public void testStateStoreFailure() throws IOException {
        stateManager.setFailOnStateSet(Scope.LOCAL, true);

        session.setState(Collections.singletonMap("abc", "123"), Scope.LOCAL);
        stateManager.assertStateNotSet();

        // Should not throw exception
        session.commit();

        // No longer fail on state updates
        stateManager.setFailOnStateSet(Scope.LOCAL, false);
        session.commit();
        stateManager.assertStateNotSet();

        session.setState(Collections.singletonMap("abc", "123"), Scope.LOCAL);
        stateManager.assertStateNotSet();
        session.commit();
        stateManager.assertStateEquals("abc", "123", Scope.LOCAL);
        stateManager.assertStateNotSet(Scope.CLUSTER);
    }

    @Test
    public void testStateRetrievedHasVersion() throws IOException {
        StateMap retrieved = session.getState(Scope.LOCAL);
        assertNotNull(retrieved);
        assertEquals(-1, retrieved.getVersion());
        assertEquals(1, stateManager.getRetrievalCount(Scope.LOCAL));
        assertEquals(0, stateManager.getRetrievalCount(Scope.CLUSTER));

        session.setState(Collections.singletonMap("abc", "123"), Scope.LOCAL);
        stateManager.assertStateNotSet();


        retrieved = session.getState(Scope.LOCAL);
        assertNotNull(retrieved);
        assertEquals(0, retrieved.getVersion());
        assertEquals(Collections.singletonMap("abc", "123"), retrieved.toMap());

        session.setState(Collections.singletonMap("abc", "222"), Scope.LOCAL);
        retrieved = session.getState(Scope.LOCAL);
        assertNotNull(retrieved);
        assertEquals(1, retrieved.getVersion());

        session.commit();
        stateManager.assertStateEquals("abc", "222", Scope.LOCAL);
        stateManager.assertStateNotSet(Scope.CLUSTER);

        retrieved = session.getState(Scope.LOCAL);
        assertNotNull(retrieved);
        assertEquals(1, retrieved.getVersion());
    }

    @Test
    public void testRollbackDoesNotStoreState() throws IOException {
        session.setState(Collections.singletonMap("abc", "1"), Scope.LOCAL);
        session.rollback();
        stateManager.assertStateNotSet();

        session.commit();
        stateManager.assertStateNotSet();
    }

    @Test
    public void testCheckpointedStateStoredOnCommit() throws IOException {
        session.setState(Collections.singletonMap("abc", "1"), Scope.LOCAL);
        stateManager.assertStateNotSet();

        session.checkpoint();
        session.commit();

        stateManager.assertStateEquals("abc", "1", Scope.LOCAL);
    }

    @Test
    public void testSessionStateStoredOverCheckpoint() throws IOException {
        session.setState(Collections.singletonMap("abc", "1"), Scope.LOCAL);
        stateManager.assertStateNotSet();

        session.checkpoint();
        session.setState(Collections.singletonMap("abc", "2"), Scope.LOCAL);

        session.commit();
        stateManager.assertStateEquals("abc", "2", Scope.LOCAL);
    }


    @Test
    public void testRollbackAfterCheckpointStoresState() throws IOException {
        session.setState(Collections.singletonMap("abc", "1"), Scope.LOCAL);
        stateManager.assertStateNotSet();

        session.checkpoint();

        session.setState(Collections.singletonMap("abc", "2"), Scope.LOCAL);
        session.rollback();

        stateManager.assertStateNotSet();
        session.commit();

        stateManager.assertStateEquals("abc", "1", Scope.LOCAL);
    }

    @Test
    public void testFullRollbackAFterCheckpointDoesNotStoreState() throws IOException {
        session.setState(Collections.singletonMap("abc", "1"), Scope.LOCAL);
        stateManager.assertStateNotSet();

        session.checkpoint();

        session.setState(Collections.singletonMap("abc", "2"), Scope.LOCAL);
        session.rollback(true, true);
        session.commit();

        stateManager.assertStateNotSet();
    }

    private static class MockFlowFileRepository implements FlowFileRepository {

        private boolean failOnUpdate = false;
        private final AtomicLong idGenerator = new AtomicLong(0L);
        private final List<RepositoryRecord> updates = new ArrayList<>();
        private final ContentRepository contentRepo;

        public MockFlowFileRepository(final ContentRepository contentRepo) {
            this.contentRepo = contentRepo;
        }

        public void setFailOnUpdate(final boolean fail) {
            this.failOnUpdate = fail;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public long getNextFlowFileSequence() {
            return idGenerator.getAndIncrement();
        }

        @Override
        public long getMaxFlowFileIdentifier() throws IOException {
            return 0L;
        }

        @Override
        public void updateMaxFlowFileIdentifier(final long maxId) {
        }

        @Override
        public void updateRepository(Collection<RepositoryRecord> records) throws IOException {
            if (failOnUpdate) {
                throw new IOException("FlowFile Repository told to fail on update for unit test");
            }
            updates.addAll(records);

            for (final RepositoryRecord record : records) {
                if (record.getType() == RepositoryRecordType.DELETE) {
                    contentRepo.decrementClaimantCount(record.getCurrentClaim());
                }

                if (!Objects.equals(record.getOriginalClaim(), record.getCurrentClaim())) {
                    contentRepo.decrementClaimantCount(record.getOriginalClaim());
                }
            }
        }

        public List<RepositoryRecord> getUpdates() {
            return updates;
        }

        @Override
        public long getStorageCapacity() throws IOException {
            return 0;
        }

        @Override
        public long getUsableStorageSpace() throws IOException {
            return 0;
        }

        @Override
        public String getFileStoreName() {
            return null;
        }

        @Override
        public boolean isVolatile() {
            return false;
        }

        @Override
        public long loadFlowFiles(QueueProvider queueProvider) throws IOException {
            return 0;
        }

        @Override
        public Set<String> findQueuesWithFlowFiles(final FlowFileSwapManager flowFileSwapManager) throws IOException {
            return Collections.emptySet();
        }

        @Override
        public void swapFlowFilesIn(String swapLocation, List<FlowFileRecord> flowFileRecords, FlowFileQueue queue) throws IOException {
        }

        @Override
        public void swapFlowFilesOut(List<FlowFileRecord> swappedOut, FlowFileQueue queue, String swapLocation) throws IOException {
        }

        @Override
        public void initialize(ResourceClaimManager claimManager) throws IOException {
        }

        @Override
        public boolean isValidSwapLocationSuffix(final String swapLocationSuffix) {
            return false;
        }
    }

    private static class MockContentRepository implements ContentRepository {

        private final AtomicLong idGenerator = new AtomicLong(0L);
        private final AtomicLong claimsRemoved = new AtomicLong(0L);
        private ResourceClaimManager claimManager;
        private boolean disableRead = false;

        private final ConcurrentMap<ContentClaim, AtomicInteger> claimantCounts = new ConcurrentHashMap<>();

        @Override
        public void shutdown() {
        }

        public Set<ContentClaim> getExistingClaims() {
            final Set<ContentClaim> claims = new HashSet<>();

            for (final Map.Entry<ContentClaim, AtomicInteger> entry : claimantCounts.entrySet()) {
                if (entry.getValue().get() > 0) {
                    claims.add(entry.getKey());
                }
            }

            return claims;
        }

        @Override
        public ContentClaim create(boolean lossTolerant) throws IOException {
            final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", String.valueOf(idGenerator.getAndIncrement()), false, false);
            final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 0L);

            claimantCounts.put(contentClaim, new AtomicInteger(1));
            final Path path = getPath(contentClaim);
            final Path parent = path.getParent();
            if (Files.exists(parent) == false) {
                Files.createDirectories(parent);
            }
            Files.createFile(getPath(contentClaim));
            return contentClaim;
        }

        public ContentClaim create(byte[] content) throws IOException {
            final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", String.valueOf(idGenerator.getAndIncrement()), false, false);
            final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 0L);

            claimantCounts.put(contentClaim, new AtomicInteger(1));
            final Path path = getPath(contentClaim);
            final Path parent = path.getParent();
            if (Files.exists(parent) == false) {
                Files.createDirectories(parent);
            }

            try (final OutputStream out = new FileOutputStream(getPath(contentClaim).toFile())) {
                out.write(content);
            }
            return contentClaim;
        }

        @Override
        public int incrementClaimaintCount(ContentClaim claim) {
            AtomicInteger count = claimantCounts.get(claim);
            if (count == null) {
                count = new AtomicInteger(0);
            }
            return count.incrementAndGet();
        }

        @Override
        public int getClaimantCount(ContentClaim claim) {
            final AtomicInteger count = claimantCounts.get(claim);
            if (count == null) {
                throw new IllegalArgumentException("Unknown Claim: " + claim);
            }
            return count.get();
        }

        public long getClaimsRemoved() {
            return claimsRemoved.get();
        }

        @Override
        public long getContainerCapacity(String containerName) throws IOException {
            return 0;
        }

        @Override
        public Set<String> getContainerNames() {
            return new HashSet<>();
        }

        @Override
        public long getContainerUsableSpace(String containerName) throws IOException {
            return 0;
        }

        @Override
        public String getContainerFileStoreName(String containerName) {
            return null;
        }

        @Override
        public int decrementClaimantCount(ContentClaim claim) {
            if (claim == null) {
                return 0;
            }

            final AtomicInteger count = claimantCounts.get(claim);
            if (count == null) {
                return 0;
            }
            final int newClaimantCount = count.decrementAndGet();

            if (newClaimantCount < 0) {
                throw new IllegalStateException("Content Claim removed, resulting in a claimant count of " + newClaimantCount + " for " + claim);
            }

            claimsRemoved.getAndIncrement();

            return newClaimantCount;
        }

        @Override
        public boolean remove(ContentClaim claim) {
            return true;
        }

        @Override
        public ContentClaim clone(ContentClaim original, boolean lossTolerant) throws IOException {
            return null;
        }

        @Override
        public long merge(Collection<ContentClaim> claims, ContentClaim destination, byte[] header, byte[] footer, byte[] demarcator) throws IOException {
            return 0;
        }

        private Path getPath(final ContentClaim contentClaim) {
            final ResourceClaim claim = contentClaim.getResourceClaim();
            return getPath(claim);
        }

        private Path getPath(final ResourceClaim claim) {
            return Paths.get("target").resolve("contentRepo").resolve(claim.getContainer()).resolve(claim.getSection()).resolve(claim.getId());
        }

        @Override
        public long importFrom(Path content, ContentClaim claim) throws IOException {
            Files.copy(content, getPath(claim));
            final long size = Files.size(content);
            ((StandardContentClaim) claim).setLength(size);
            return size;
        }

        @Override
        public long importFrom(InputStream content, ContentClaim claim) throws IOException {
            Files.copy(content, getPath(claim));
            final long size = Files.size(getPath(claim));
            ((StandardContentClaim) claim).setLength(size);
            return size;
        }

        @Override
        public long exportTo(ContentClaim claim, Path destination, boolean append) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long exportTo(ContentClaim claim, Path destination, boolean append, long offset, long length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long exportTo(ContentClaim claim, OutputStream destination) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long exportTo(ContentClaim claim, OutputStream destination, long offset, long length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size(ContentClaim claim) throws IOException {
            return Files.size(getPath(claim));
        }

        @Override
        public InputStream read(ContentClaim claim) throws IOException {
            if (disableRead) {
                throw new IOException("Reading from repo is disabled by unit test");
            }

            if (claim == null) {
                return new ByteArrayInputStream(new byte[0]);
            }

            try {
                return new FileInputStream(getPath(claim).toFile());
            } catch (final FileNotFoundException fnfe) {
                throw new ContentNotFoundException(claim, fnfe);
            }
        }

        @Override
        public InputStream read(final ResourceClaim claim) throws IOException {
            if (disableRead) {
                throw new IOException("Reading from repo is disabled by unit test");
            }

            if (claim == null) {
                return new ByteArrayInputStream(new byte[0]);
            }

            try {
                return new FileInputStream(getPath(claim).toFile());
            } catch (final FileNotFoundException fnfe) {
                throw new ContentNotFoundException(null, fnfe);
            }
        }

        @Override
        public OutputStream write(final ContentClaim claim) throws IOException {
            final Path path = getPath(claim);
            final File file = path.toFile();
            final File parentFile = file.getParentFile();

            if (!parentFile.exists() && !parentFile.mkdirs()) {
                throw new IOException("Unable to create directory " + parentFile.getAbsolutePath());
            }

            final OutputStream fos = new FileOutputStream(file);
            return new FilterOutputStream(fos) {
                @Override
                public void write(final int b) throws IOException {
                    fos.write(b);
                    ((StandardContentClaim) claim).setLength(claim.getLength() + 1);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    fos.write(b, off, len);
                    ((StandardContentClaim) claim).setLength(claim.getLength() + len);
                }

                @Override
                public void write(byte[] b) throws IOException {
                    fos.write(b);
                    ((StandardContentClaim) claim).setLength(claim.getLength() + b.length);
                }

                @Override
                public void close() throws IOException {
                    super.close();
                }
            };
        }

        @Override
        public void purge() {
        }

        @Override
        public void cleanup() {
        }

        @Override
        public boolean isAccessible(ContentClaim contentClaim) throws IOException {
            return true;
        }

        @Override
        public void initialize(ResourceClaimManager claimManager) throws IOException {
            this.claimManager = claimManager;
        }
    }
}
