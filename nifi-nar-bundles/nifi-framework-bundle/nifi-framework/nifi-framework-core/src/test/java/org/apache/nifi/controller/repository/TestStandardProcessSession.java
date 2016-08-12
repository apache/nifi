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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.StandardFlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.MissingFlowFileException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestStandardProcessSession {

    private StandardProcessSession session;
    private MockContentRepository contentRepo;
    private FlowFileQueue flowFileQueue;
    private ProcessContext context;

    private ProvenanceEventRepository provenanceRepo;
    private MockFlowFileRepository flowFileRepo;
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
    @SuppressWarnings("unchecked")
    public void setup() throws IOException {
        resourceClaimManager = new StandardResourceClaimManager();

        System.setProperty("nifi.properties.file.path", "src/test/resources/nifi.properties");
        final FlowFileEventRepository flowFileEventRepo = Mockito.mock(FlowFileEventRepository.class);
        final CounterRepository counterRepo = Mockito.mock(CounterRepository.class);
        provenanceRepo = new MockProvenanceRepository();

        final Connection connection = Mockito.mock(Connection.class);
        final ProcessScheduler processScheduler = Mockito.mock(ProcessScheduler.class);

        final FlowFileSwapManager swapManager = Mockito.mock(FlowFileSwapManager.class);
        final StandardFlowFileQueue actualQueue = new StandardFlowFileQueue("1", connection, flowFileRepo, provenanceRepo, null, processScheduler, swapManager, null, 10000);
        flowFileQueue = Mockito.spy(actualQueue);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                flowFileQueue.put((FlowFileRecord) invocation.getArguments()[0]);
                return null;
            }
        }).when(connection).enqueue(Mockito.any(FlowFileRecord.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                flowFileQueue.putAll((Collection<FlowFileRecord>) invocation.getArguments()[0]);
                return null;
            }
        }).when(connection).enqueue(Mockito.any(Collection.class));

        final Connectable dest = Mockito.mock(Connectable.class);
        when(connection.getDestination()).thenReturn(dest);
        when(connection.getSource()).thenReturn(dest);

        final List<Connection> connList = new ArrayList<>();
        connList.add(connection);

        final ProcessGroup procGroup = Mockito.mock(ProcessGroup.class);
        when(procGroup.getIdentifier()).thenReturn("proc-group-identifier-1");

        final Connectable connectable = Mockito.mock(Connectable.class);
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
                } else if (relationship == FAKE_RELATIONSHIP || relationship.equals(FAKE_RELATIONSHIP) ){
                    return null;
                }else {
                    return new HashSet<>(connList);
                }
            }
        }).when(connectable).getConnections(Mockito.any(Relationship.class));

        when(connectable.getConnections()).thenReturn(new HashSet<>(connList));

        contentRepo = new MockContentRepository();
        contentRepo.initialize(new StandardResourceClaimManager());
        flowFileRepo = new MockFlowFileRepository();

        context = new ProcessContext(connectable, new AtomicLong(0L), contentRepo, flowFileRepo, flowFileEventRepo, counterRepo, provenanceRepo);
        session = new StandardProcessSession(context);
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
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            outputStream.write(0);
            Assert.fail("Expected OutputStream to be disabled; was able to call write(int)");
        } catch (final Exception ex) {
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            outputStream.write(new byte[0], 0, 0);
            Assert.fail("Expected OutputStream to be disabled; was able to call write(byte[], int, int)");
        } catch (final Exception ex) {
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
        }
    }

    private void assertDisabled(final InputStream inputStream) {
        try {
            inputStream.read();
            Assert.fail("Expected InputStream to be disabled; was able to call read()");
        } catch (final Exception ex) {
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.read(new byte[0]);
            Assert.fail("Expected InputStream to be disabled; was able to call read(byte[])");
        } catch (final Exception ex) {
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.read(new byte[0], 0, 0);
            Assert.fail("Expected InputStream to be disabled; was able to call read(byte[], int, int)");
        } catch (final Exception ex) {
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.reset();
            Assert.fail("Expected InputStream to be disabled; was able to call reset()");
        } catch (final Exception ex) {
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
        }
        try {
            inputStream.skip(1L);
            Assert.fail("Expected InputStream to be disabled; was able to call skip(long)");
        } catch (final Exception ex) {
            Assert.assertEquals(FlowFileAccessException.class, ex.getClass());
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
        session.read(flowFile, true , new InputStreamCallback() {
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
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))
            .size(1L)
            .build();
        flowFileQueue.put(flowFileRecord);

        // attempt to read the data.
        try {
            final FlowFile ff1 = session.get();

            session.read(ff1, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
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
                    final Set<FlowFileRecord> expired = invocation.getArgumentAt(1, Set.class);
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
    public void testManyFilesOpened() throws IOException {

        StandardProcessSession[] standardProcessSessions = new StandardProcessSession[100000];
        for(int i = 0; i<70000;i++){
            standardProcessSessions[i] = new StandardProcessSession(context);

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
            } catch (Exception e){
                System.out.println("Failed at file:"+i);
                throw e;
            }
            if(i%1000==0){
                System.out.println("i:"+i);
            }
        }
    }

    @Test
    public void testMissingFlowFileExceptionThrownWhenUnableToReadDataStreamCallback() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))
            .size(1L)
            .build();
        flowFileQueue.put(flowFileRecord);

        // attempt to read the data.
        try {
            final FlowFile ff1 = session.get();

            session.write(ff1, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                }
            });
            Assert.fail("Expected MissingFlowFileException");
        } catch (final MissingFlowFileException mffe) {
        }
    }

    @Test
    public void testContentNotFoundExceptionThrownWhenUnableToReadDataStreamCallbackOffsetTooLarge() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))
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
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))
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
                }
            });
            Assert.fail("Expected ContentNotFoundException");
        } catch (final MissingFlowFileException mffe) {
        }
    }

    @Test
    public void testContentNotFoundExceptionThrownWhenUnableToReadDataOffsetTooLarge() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))
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
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))

        .contentClaimOffset(1000L).size(1L).build();
        flowFileQueue.put(flowFileRecord2);

        // attempt to read the data.
        try {
            session.get();
            final FlowFile ff2 = session.get();
            session.read(ff2, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
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
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))

        .contentClaimOffset(0L).size(0L).build();
        flowFileQueue.put(flowFileRecord);

        final FlowFile originalFlowFile = session.get();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertEquals(1, flowFileQueue.getUnacknowledgedQueueSize().getObjectCount());

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
        assertEquals(0, flowFileQueue.getUnacknowledgedQueueSize().getObjectCount());
    }

    @Test
    public void testRollbackAfterCheckpoint() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
            .entryDate(System.currentTimeMillis())
            .contentClaim(new StandardContentClaim(resourceClaimManager.newResourceClaim("x", "x", "0", true), 0L))

        .contentClaimOffset(0L).size(0L).build();
        flowFileQueue.put(flowFileRecord);

        final FlowFile originalFlowFile = session.get();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertEquals(1, flowFileQueue.getUnacknowledgedQueueSize().getObjectCount());

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
        assertEquals(0, flowFileQueue.getUnacknowledgedQueueSize().getObjectCount());

        session.rollback();

        flowFileQueue.put(flowFileRecord);
        assertFalse(flowFileQueue.isActiveQueueEmpty());

        final FlowFile originalRound2 = session.get();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertEquals(1, flowFileQueue.getUnacknowledgedQueueSize().getObjectCount());

        final FlowFile modifiedRound2 = session.write(originalRound2, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write("Hello".getBytes());
            }
        });

        session.transfer(modifiedRound2);

        session.checkpoint();
        assertTrue(flowFileQueue.isActiveQueueEmpty());
        assertEquals(1, flowFileQueue.getUnacknowledgedQueueSize().getObjectCount());

        session.commit();

        // FlowFile transferred back to queue
        assertEquals(1, flowFileQueue.size().getObjectCount());
        assertEquals(0, flowFileQueue.getUnacknowledgedQueueSize().getObjectCount());
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

    private static class MockFlowFileRepository implements FlowFileRepository {
        private boolean failOnUpdate = false;
        private final AtomicLong idGenerator = new AtomicLong(0L);

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
        public void updateRepository(Collection<RepositoryRecord> records) throws IOException {
            if (failOnUpdate) {
                throw new IOException("FlowFile Repository told to fail on update for unit test");
            }
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
        public boolean isVolatile() {
            return false;
        }

        @Override
        public long loadFlowFiles(QueueProvider queueProvider, long minimumSequenceNumber) throws IOException {
            return 0;
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

            for (long i = 0; i < idGenerator.get(); i++) {
                final ResourceClaim resourceClaim = resourceClaimManager.newResourceClaim("container", "section", String.valueOf(i), false);
                final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 0L);
                if (getClaimantCount(contentClaim) > 0) {
                    claims.add(contentClaim);
                }
            }

            return claims;
        }

        @Override
        public ContentClaim create(boolean lossTolerant) throws IOException {
            final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", String.valueOf(idGenerator.getAndIncrement()), false);
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

        @Override
        public int incrementClaimaintCount(ContentClaim claim) {
            final AtomicInteger count = claimantCounts.get(claim);
            if (count == null) {
                throw new IllegalArgumentException("Unknown Claim: " + claim);
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
            return new HashSet<String>();
        }

        @Override
        public long getContainerUsableSpace(String containerName) throws IOException {
            return 0;
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
