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
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowFileQueue;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.StandardFlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaimManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.MissingFlowFileException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.MockProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
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

    private ProvenanceEventRepository provenanceRepo;
    private MockFlowFileRepository flowFileRepo;

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
        System.setProperty("nifi.properties.file.path", "src/test/resources/nifi.properties");
        final FlowFileEventRepository flowFileEventRepo = Mockito.mock(FlowFileEventRepository.class);
        final CounterRepository counterRepo = Mockito.mock(CounterRepository.class);
        provenanceRepo = new MockProvenanceEventRepository();

        final Connection connection = Mockito.mock(Connection.class);
        final ProcessScheduler processScheduler = Mockito.mock(ProcessScheduler.class);

        flowFileQueue = new StandardFlowFileQueue("1", connection, processScheduler, 10000);
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

        Mockito.doAnswer(new Answer<Set<Connection>>() {
            @Override
            public Set<Connection> answer(final InvocationOnMock invocation) throws Throwable {
                final Object[] arguments = invocation.getArguments();
                final Relationship relationship = (Relationship) arguments[0];
                if (relationship == Relationship.SELF) {
                    return Collections.emptySet();
                } else {
                    return new HashSet<>(connList);
                }
            }
        }).when(connectable).getConnections(Mockito.any(Relationship.class));
        when(connectable.getConnections()).thenReturn(new HashSet<>(connList));

        contentRepo = new MockContentRepository();
        contentRepo.initialize(new StandardContentClaimManager());
        flowFileRepo = new MockFlowFileRepository();

        final ProcessContext context = new ProcessContext(connectable, new AtomicLong(0L), contentRepo, flowFileRepo, flowFileEventRepo, counterRepo, provenanceRepo);
        session = new StandardProcessSession(context);
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

        FlowFile flowFile3 = session.create();
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

        FlowFile orig = session.get();
        FlowFile newFlowFile = session.create(orig);
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

        FlowFile orig = session.get();
        FlowFile newFlowFile = session.create(orig);
        session.transfer(newFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        assertEquals(1, provenanceRepo.getEvents(0L, 100000).size());  // 1 event for both parents and children
    }

    @Test
    public void testProvenanceEventsEmittedForRemove() throws IOException {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .build();

        flowFileQueue.put(flowFileRecord);

        FlowFile orig = session.get();
        FlowFile newFlowFile = session.create(orig);
        FlowFile secondNewFlowFile = session.create(orig);
        session.remove(newFlowFile);
        session.transfer(secondNewFlowFile, new Relationship.Builder().name("A").build());
        session.commit();

        assertEquals(1, provenanceRepo.getEvents(0L, 100000).size());
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
        
        for ( final ProvenanceEventRecord event : events ) {
            switch (event.getEventType()) {
                case JOIN:
                    assertEquals(child.getAttribute("uuid"), event.getFlowFileUuid());
                    joinCount++;
                    break;
                case ATTRIBUTES_MODIFIED:
                    if ( event.getFlowFileUuid().equals(ff1.getAttribute("uuid")) ) {
                        ff1UpdateCount++;
                    } else if ( event.getFlowFileUuid().equals(ff2.getAttribute("uuid")) ) {
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

        FlowFile orig = session.get();
        FlowFile newFlowFile = session.create(orig);
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
        FlowFile ff1 = session.create();

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
        FlowFile ff1 = session.create();

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
                .contentClaim(new ContentClaim() {
                    @Override
                    public int compareTo(ContentClaim arg0) {
                        return 0;
                    }

                    @Override
                    public String getId() {
                        return "0";
                    }

                    @Override
                    public String getContainer() {
                        return "x";
                    }

                    @Override
                    public String getSection() {
                        return "x";
                    }

                    @Override
                    public boolean isLossTolerant() {
                        return true;
                    }
                })
                .size(1L)
                .build();
        flowFileQueue.put(flowFileRecord);

        // attempt to read the data.
        try {
            FlowFile ff1 = session.get();

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
    public void testMissingFlowFileExceptionThrownWhenUnableToReadDataStreamCallback() {
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                .addAttribute("uuid", "12345678-1234-1234-1234-123456789012")
                .entryDate(System.currentTimeMillis())
                .contentClaim(new ContentClaim() {
                    @Override
                    public int compareTo(ContentClaim arg0) {
                        return 0;
                    }

                    @Override
                    public String getId() {
                        return "0";
                    }

                    @Override
                    public String getContainer() {
                        return "x";
                    }

                    @Override
                    public String getSection() {
                        return "x";
                    }

                    @Override
                    public boolean isLossTolerant() {
                        return true;
                    }
                })
                .size(1L)
                .build();
        flowFileQueue.put(flowFileRecord);

        // attempt to read the data.
        try {
            FlowFile ff1 = session.get();

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
                .contentClaim(new ContentClaim() {
                    @Override
                    public int compareTo(ContentClaim arg0) {
                        return 0;
                    }

                    @Override
                    public String getId() {
                        return "0";
                    }

                    @Override
                    public String getContainer() {
                        return "container";
                    }

                    @Override
                    public String getSection() {
                        return "section";
                    }

                    @Override
                    public boolean isLossTolerant() {
                        return true;
                    }
                })
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
                .contentClaim(new ContentClaim() {
                    @Override
                    public int compareTo(ContentClaim arg0) {
                        return 0;
                    }

                    @Override
                    public String getId() {
                        return "0";
                    }

                    @Override
                    public String getContainer() {
                        return "container";
                    }

                    @Override
                    public String getSection() {
                        return "section";
                    }

                    @Override
                    public boolean isLossTolerant() {
                        return true;
                    }
                })
                .contentClaimOffset(1000L)
                .size(1000L)
                .build();
        flowFileQueue.put(flowFileRecord2);

        // attempt to read the data.
        try {
            session.get();
            FlowFile ff2 = session.get();
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
            .contentClaim(new ContentClaim() {
                @Override
                public int compareTo(ContentClaim arg0) {
                    return 0;
                }
    
                @Override
                public String getId() {
                    return "0";
                }
    
                @Override
                public String getContainer() {
                    return "container";
                }
    
                @Override
                public String getSection() {
                    return "section";
                }
    
                @Override
                public boolean isLossTolerant() {
                    return true;
                }
            }).build();
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
            .contentClaim(new ContentClaim() {
                @Override
                public int compareTo(ContentClaim arg0) {
                    return 0;
                }
    
                @Override
                public String getId() {
                    return "0";
                }
    
                @Override
                public String getContainer() {
                    return "container";
                }
    
                @Override
                public String getSection() {
                    return "section";
                }
    
                @Override
                public boolean isLossTolerant() {
                    return true;
                }
            })
            .contentClaimOffset(1000L).size(1L).build();
        flowFileQueue.put(flowFileRecord2);

        // attempt to read the data.
        try {
            session.get();
            FlowFile ff2 = session.get();
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
        FlowFile ff1 = session.create();

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
    public void testCreateEmitted() throws IOException {
        FlowFile newFlowFile = session.create();
        session.transfer(newFlowFile, new Relationship.Builder().name("A").build());
        session.commit();
        
        final List<ProvenanceEventRecord> events = provenanceRepo.getEvents(0L, 10000);
        assertFalse(events.isEmpty());
        assertEquals(1, events.size());
        
        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.CREATE, event.getEventType());
    }
    
    private static class MockFlowFileRepository implements FlowFileRepository {

        private final AtomicLong idGenerator = new AtomicLong(0L);

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
        public void initialize(ContentClaimManager claimManager) throws IOException {
        }
    }

    private static class MockContentRepository implements ContentRepository {

        private final AtomicLong idGenerator = new AtomicLong(0L);
        private final AtomicLong claimsRemoved = new AtomicLong(0L);
        private ContentClaimManager claimManager;

        private ConcurrentMap<ContentClaim, AtomicInteger> claimantCounts = new ConcurrentHashMap<>();

        public Set<ContentClaim> getExistingClaims() {
            final Set<ContentClaim> claims = new HashSet<>();

            for (long i = 0; i < idGenerator.get(); i++) {
                final ContentClaim claim = claimManager.newContentClaim("container", "section", String.valueOf(i), false);
                if (getClaimantCount(claim) > 0) {
                    claims.add(claim);
                }
            }

            return claims;
        }

        @Override
        public ContentClaim create(boolean lossTolerant) throws IOException {
            final ContentClaim claim = claimManager.newContentClaim("container", "section", String.valueOf(idGenerator.getAndIncrement()), false);
            claimantCounts.put(claim, new AtomicInteger(1));
            return claim;
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

        private Path getPath(final ContentClaim claim) {
            return Paths.get("target").resolve("contentRepo").resolve(claim.getContainer()).resolve(claim.getSection()).resolve(claim.getId());
        }

        @Override
        public long importFrom(Path content, ContentClaim claim) throws IOException {
            Files.copy(content, getPath(claim));
            return Files.size(content);
        }

        @Override
        public long importFrom(Path content, ContentClaim claim, boolean append) throws IOException {
            if (append) {
                throw new UnsupportedOperationException();
            }
            return importFrom(content, claim);
        }

        @Override
        public long importFrom(InputStream content, ContentClaim claim) throws IOException {
            Files.copy(content, getPath(claim));
            return Files.size(getPath(claim));
        }

        @Override
        public long importFrom(InputStream content, ContentClaim claim, boolean append) throws IOException {
            if (append) {
                throw new UnsupportedOperationException();
            }
            return importFrom(content, claim);
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
        public OutputStream write(ContentClaim claim) throws IOException {
            final Path path = getPath(claim);
            final File file = path.toFile();
            final File parentFile = file.getParentFile();

            if (!parentFile.exists() && !parentFile.mkdirs()) {
                throw new IOException("Unable to create directory " + parentFile.getAbsolutePath());
            }
            return new FileOutputStream(file);
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
        public void initialize(ContentClaimManager claimManager) throws IOException {
            this.claimManager = claimManager;
        }
    }
}
