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

package org.apache.nifi.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestStandardFlowFileQueue {
    private TestSwapManager swapManager = null;
    private StandardFlowFileQueue queue = null;

    @Before
    public void setup() {
        final Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getSource()).thenReturn(Mockito.mock(Connectable.class));
        Mockito.when(connection.getDestination()).thenReturn(Mockito.mock(Connectable.class));

        final ProcessScheduler scheduler = Mockito.mock(ProcessScheduler.class);
        swapManager = new TestSwapManager();

        final FlowFileRepository flowFileRepo = Mockito.mock(FlowFileRepository.class);
        final ProvenanceEventRepository provRepo = Mockito.mock(ProvenanceEventRepository.class);
        final ResourceClaimManager claimManager = Mockito.mock(ResourceClaimManager.class);

        Mockito.when(provRepo.eventBuilder()).thenReturn(new StandardProvenanceEventRecord.Builder());

        queue = new StandardFlowFileQueue("id", connection, flowFileRepo, provRepo, claimManager, scheduler, swapManager, null, 10000);
        TestFlowFile.idGenerator.set(0L);
    }


    @Test
    public void testSwapOutOccurs() {
        for (int i = 0; i < 10000; i++) {
            queue.put(new TestFlowFile());
            assertEquals(0, swapManager.swapOutCalledCount);
            assertEquals(i + 1, queue.size().getObjectCount());
            assertEquals(i + 1, queue.size().getByteCount());
        }

        for (int i = 0; i < 9999; i++) {
            queue.put(new TestFlowFile());
            assertEquals(0, swapManager.swapOutCalledCount);
            assertEquals(i + 10001, queue.size().getObjectCount());
            assertEquals(i + 10001, queue.size().getByteCount());
        }

        queue.put(new TestFlowFile(1000));
        assertEquals(1, swapManager.swapOutCalledCount);
        assertEquals(20000, queue.size().getObjectCount());
        assertEquals(20999, queue.size().getByteCount());

        assertEquals(10000, queue.getActiveQueueSize().getObjectCount());
    }

    @Test
    public void testLowestPrioritySwappedOutFirst() {
        final List<FlowFilePrioritizer> prioritizers = new ArrayList<>();
        prioritizers.add(new FlowFileSizePrioritizer());
        queue.setPriorities(prioritizers);

        long maxSize = 20000;
        for (int i = 1; i <= 20000; i++) {
            queue.put(new TestFlowFile(maxSize - i));
        }

        assertEquals(1, swapManager.swapOutCalledCount);
        assertEquals(20000, queue.size().getObjectCount());

        assertEquals(10000, queue.getActiveQueueSize().getObjectCount());
        final List<FlowFileRecord> flowFiles = queue.poll(Integer.MAX_VALUE, new HashSet<FlowFileRecord>());
        assertEquals(10000, flowFiles.size());
        for (int i = 0; i < 10000; i++) {
            assertEquals(i, flowFiles.get(i).getSize());
        }
    }

    @Test
    public void testSwapIn() {
        for (int i = 1; i <= 20000; i++) {
            queue.put(new TestFlowFile());
        }

        assertEquals(1, swapManager.swappedOut.size());
        queue.put(new TestFlowFile());
        assertEquals(1, swapManager.swappedOut.size());

        final Set<FlowFileRecord> exp = new HashSet<>();
        for (int i = 0; i < 9999; i++) {
            assertNotNull(queue.poll(exp));
        }

        assertEquals(0, swapManager.swapInCalledCount);
        assertEquals(1, queue.getActiveQueueSize().getObjectCount());
        assertNotNull(queue.poll(exp));

        assertEquals(0, swapManager.swapInCalledCount);
        assertEquals(0, queue.getActiveQueueSize().getObjectCount());

        assertEquals(1, swapManager.swapOutCalledCount);

        assertNotNull(queue.poll(exp)); // this should trigger a swap-in of 10,000 records, and then pull 1 off the top.
        assertEquals(1, swapManager.swapInCalledCount);
        assertEquals(9999, queue.getActiveQueueSize().getObjectCount());

        assertTrue(swapManager.swappedOut.isEmpty());

        queue.poll(exp);
    }

    @Test(timeout = 20000)
    public void testDropSwappedFlowFiles() {
        for (int i = 1; i <= 210000; i++) {
            queue.put(new TestFlowFile());
        }

        assertEquals(20, swapManager.swappedOut.size());
        final DropFlowFileStatus status = queue.dropFlowFiles("1", "Unit Test");
        while (status.getState() != DropFlowFileState.COMPLETE) {
            try {
                Thread.sleep(100L);
            } catch (final Exception e) {
            }
        }

        assertEquals(0, queue.size().getObjectCount());
        assertEquals(0, queue.size().getByteCount());
        assertEquals(0, swapManager.swappedOut.size());
        assertEquals(20, swapManager.swapInCalledCount);
    }

    private class TestSwapManager implements FlowFileSwapManager {
        private final Map<String, List<FlowFileRecord>> swappedOut = new HashMap<>();
        int swapOutCalledCount = 0;
        int swapInCalledCount = 0;

        @Override
        public void initialize(final SwapManagerInitializationContext initializationContext) {

        }

        @Override
        public String swapOut(List<FlowFileRecord> flowFiles, FlowFileQueue flowFileQueue) throws IOException {
            swapOutCalledCount++;
            final String location = UUID.randomUUID().toString();
            swappedOut.put(location, new ArrayList<FlowFileRecord>(flowFiles));
            return location;
        }

        @Override
        public List<FlowFileRecord> peek(String swapLocation, final FlowFileQueue flowFileQueue) throws IOException {
            return new ArrayList<FlowFileRecord>(swappedOut.get(swapLocation));
        }

        @Override
        public List<FlowFileRecord> swapIn(String swapLocation, FlowFileQueue flowFileQueue) throws IOException {
            swapInCalledCount++;
            return swappedOut.remove(swapLocation);
        }

        @Override
        public List<String> recoverSwapLocations(FlowFileQueue flowFileQueue) throws IOException {
            return new ArrayList<String>(swappedOut.keySet());
        }

        @Override
        public QueueSize getSwapSize(String swapLocation) throws IOException {
            final List<FlowFileRecord> flowFiles = swappedOut.get(swapLocation);
            if (flowFiles == null) {
                return new QueueSize(0, 0L);
            }

            int count = 0;
            long size = 0L;
            for (final FlowFileRecord flowFile : flowFiles) {
                count++;
                size += flowFile.getSize();
            }

            return new QueueSize(count, size);
        }

        @Override
        public Long getMaxRecordId(String swapLocation) throws IOException {
            final List<FlowFileRecord> flowFiles = swappedOut.get(swapLocation);
            if (flowFiles == null) {
                return null;
            }

            Long max = null;
            for (final FlowFileRecord flowFile : flowFiles) {
                if (max == null || flowFile.getId() > max) {
                    max = flowFile.getId();
                }
            }

            return max;
        }

        @Override
        public void purge() {
            swappedOut.clear();
        }
    }


    private static class TestFlowFile implements FlowFileRecord {
        private static final AtomicLong idGenerator = new AtomicLong(0L);

        private final long id = idGenerator.getAndIncrement();
        private final long entryDate = System.currentTimeMillis();
        private final Map<String, String> attributes;
        private final long size;

        public TestFlowFile() {
            this(1L);
        }

        public TestFlowFile(final long size) {
            this(new HashMap<String, String>(), size);
        }

        public TestFlowFile(final Map<String, String> attributes, final long size) {
            this.attributes = attributes;
            this.size = size;

            if (!attributes.containsKey(CoreAttributes.UUID.key())) {
                attributes.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());
            }
        }


        @Override
        public long getId() {
            return id;
        }

        @Override
        public long getEntryDate() {
            return entryDate;
        }

        @Override
        public long getLineageStartDate() {
            return entryDate;
        }

        @Override
        public Long getLastQueueDate() {
            return null;
        }

        @Override
        public Set<String> getLineageIdentifiers() {
            return Collections.emptySet();
        }

        @Override
        public boolean isPenalized() {
            return false;
        }

        @Override
        public String getAttribute(String key) {
            return attributes.get(key);
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        public Map<String, String> getAttributes() {
            return Collections.unmodifiableMap(attributes);
        }

        @Override
        public int compareTo(final FlowFile o) {
            return Long.compare(id, o.getId());
        }

        @Override
        public long getPenaltyExpirationMillis() {
            return 0;
        }

        @Override
        public ContentClaim getContentClaim() {
            return null;
        }

        @Override
        public long getContentClaimOffset() {
            return 0;
        }
    }

    private static class FlowFileSizePrioritizer implements FlowFilePrioritizer {
        @Override
        public int compare(final FlowFile o1, final FlowFile o2) {
            return Long.compare(o1.getSize(), o2.getSize());
        }
    }
}
