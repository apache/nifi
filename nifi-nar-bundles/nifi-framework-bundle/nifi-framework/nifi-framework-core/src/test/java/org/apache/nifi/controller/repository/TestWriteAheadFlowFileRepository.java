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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.StandardFlowFileQueue;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.ListFlowFileStatus;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.swap.StandardSwapContents;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.wali.MinimalLockingWriteAheadLog;
import org.wali.WriteAheadRepository;

public class TestWriteAheadFlowFileRepository {

    @BeforeClass
    public static void setupProperties() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestWriteAheadFlowFileRepository.class.getResource("/conf/nifi.properties").getFile());
    }

    @Before
    @After
    public void clearRepo() throws IOException {
        final File target = new File("target");
        final File testRepo = new File(target, "test-repo");
        if (testRepo.exists()) {
            FileUtils.deleteFile(testRepo, true);
        }
    }


    @Test
    @Ignore("Intended only for local performance testing before/after making changes")
    public void testUpdatePerformance() throws IOException, InterruptedException {
        final FlowFileQueue queue = new FlowFileQueue() {

            @Override
            public String getIdentifier() {
                return "4444";
            }

            @Override
            public List<FlowFilePrioritizer> getPriorities() {
                return null;
            }

            @Override
            public SwapSummary recoverSwappedFlowFiles() {
                return null;
            }

            @Override
            public void purgeSwapFiles() {
            }

            @Override
            public void setPriorities(List<FlowFilePrioritizer> newPriorities) {
            }

            @Override
            public void setBackPressureObjectThreshold(long maxQueueSize) {
            }

            @Override
            public long getBackPressureObjectThreshold() {
                return 0;
            }

            @Override
            public void setBackPressureDataSizeThreshold(String maxDataSize) {
            }

            @Override
            public String getBackPressureDataSizeThreshold() {
                return null;
            }

            @Override
            public QueueSize size() {
                return null;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public boolean isActiveQueueEmpty() {
                return false;
            }

            @Override
            public QueueSize getUnacknowledgedQueueSize() {
                return null;
            }

            @Override
            public void acknowledge(FlowFileRecord flowFile) {
            }

            @Override
            public void acknowledge(Collection<FlowFileRecord> flowFiles) {
            }

            @Override
            public boolean isFull() {
                return false;
            }

            @Override
            public void put(FlowFileRecord file) {
            }

            @Override
            public void putAll(Collection<FlowFileRecord> files) {
            }

            @Override
            public FlowFileRecord poll(Set<FlowFileRecord> expiredRecords) {
                return null;
            }

            @Override
            public List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords) {
                return null;
            }

            @Override
            public long drainQueue(Queue<FlowFileRecord> sourceQueue, List<FlowFileRecord> destination, int maxResults, Set<FlowFileRecord> expiredRecords) {
                return 0;
            }

            @Override
            public List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords) {
                return null;
            }

            @Override
            public String getFlowFileExpiration() {
                return null;
            }

            @Override
            public int getFlowFileExpiration(TimeUnit timeUnit) {
                return 0;
            }

            @Override
            public void setFlowFileExpiration(String flowExpirationPeriod) {
            }

            @Override
            public DropFlowFileStatus dropFlowFiles(String requestIdentifier, String requestor) {
                return null;
            }

            @Override
            public DropFlowFileStatus getDropFlowFileStatus(String requestIdentifier) {
                return null;
            }

            @Override
            public DropFlowFileStatus cancelDropFlowFileRequest(String requestIdentifier) {
                return null;
            }

            @Override
            public ListFlowFileStatus listFlowFiles(String requestIdentifier, int maxResults) {
                return null;
            }

            @Override
            public ListFlowFileStatus getListFlowFileStatus(String requestIdentifier) {
                return null;
            }

            @Override
            public ListFlowFileStatus cancelListFlowFileRequest(String requestIdentifier) {
                return null;
            }

            @Override
            public FlowFileRecord getFlowFile(String flowFileUuid) throws IOException {
                return null;
            }

            @Override
            public void verifyCanList() throws IllegalStateException {
            }
        };


        final int numPartitions = 16;
        final int numThreads = 8;
        final int totalUpdates = 160_000_000;
        final int batchSize = 10;

        final Path path = Paths.get("target/minimal-locking-repo");
        deleteRecursively(path.toFile());
        assertTrue(path.toFile().mkdirs());

        final ResourceClaimManager claimManager = new StandardResourceClaimManager();
        final RepositoryRecordSerdeFactory serdeFactory = new RepositoryRecordSerdeFactory(claimManager);
        final WriteAheadRepository<RepositoryRecord> repo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serdeFactory, null);
        final Collection<RepositoryRecord> initialRecs = repo.recoverRecords();
        assertTrue(initialRecs.isEmpty());

        final int updateCountPerThread = totalUpdates / numThreads;

        final Thread[] threads = new Thread[numThreads];
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < numThreads; i++) {
                final Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        final List<RepositoryRecord> records = new ArrayList<>();
                        final int numBatches = updateCountPerThread / batchSize;
                        final MockFlowFile baseFlowFile = new MockFlowFile(0L);

                        for (int i = 0; i < numBatches; i++) {
                            records.clear();
                            for (int k = 0; k < batchSize; k++) {
                                final FlowFileRecord flowFile = new MockFlowFile(i % 100_000, baseFlowFile);
                                final String uuid = flowFile.getAttribute("uuid");

                                final StandardRepositoryRecord record = new StandardRepositoryRecord(null, flowFile);
                                record.setDestination(queue);
                                final Map<String, String> updatedAttrs = Collections.singletonMap("uuid", uuid);
                                record.setWorking(flowFile, updatedAttrs);

                                records.add(record);
                            }

                            try {
                                repo.update(records, false);
                            } catch (IOException e) {
                                e.printStackTrace();
                                Assert.fail(e.toString());
                            }
                        }
                    }
                });

                t.setDaemon(true);
                threads[i] = t;
            }

            final long start = System.nanoTime();
            for (final Thread t : threads) {
                t.start();
            }
            for (final Thread t : threads) {
                t.join();
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            if (j == 0) {
                System.out.println(millis + " ms to insert " + updateCountPerThread * numThreads + " updates using " + numPartitions + " partitions and " + numThreads + " threads, *as a warmup!*");
            } else {
                System.out.println(millis + " ms to insert " + updateCountPerThread * numThreads + " updates using " + numPartitions + " partitions and " + numThreads + " threads");
            }
        }
    }

    private void deleteRecursively(final File file) {
        final File[] children = file.listFiles();
        if (children != null) {
            for (final File child : children) {
                deleteRecursively(child);
            }
        }

        file.delete();
    }



    @Test
    public void testResourceClaimsIncremented() throws IOException {
        final ResourceClaimManager claimManager = new StandardResourceClaimManager();

        final TestQueueProvider queueProvider = new TestQueueProvider();
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");
        when(connection.getDestination()).thenReturn(Mockito.mock(Connectable.class));

        final FlowFileSwapManager swapMgr = new MockFlowFileSwapManager();
        final FlowFileQueue queue = new StandardFlowFileQueue("1234", connection, null, null, claimManager, null, swapMgr, null, 10000);

        when(connection.getFlowFileQueue()).thenReturn(queue);
        queueProvider.addConnection(connection);

        final ResourceClaim resourceClaim1 = claimManager.newResourceClaim("container", "section", "1", false, false);
        final ContentClaim claim1 = new StandardContentClaim(resourceClaim1, 0L);

        final ResourceClaim resourceClaim2 = claimManager.newResourceClaim("container", "section", "2", false, false);
        final ContentClaim claim2 = new StandardContentClaim(resourceClaim2, 0L);

        // Create a flowfile repo, update it once with a FlowFile that points to one resource claim. Then,
        // indicate that a FlowFile was swapped out. We should then be able to recover these FlowFiles and the
        // resource claims' counts should be updated for both the swapped out FlowFile and the non-swapped out FlowFile
        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(NiFiProperties.createBasicNiFiProperties(null, null))) {
            repo.initialize(claimManager);
            repo.loadFlowFiles(queueProvider, -1L);

            // Create a Repository Record that indicates that a FlowFile was created
            final FlowFileRecord flowFile1 = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-111111111111")
                    .contentClaim(claim1)
                    .build();
            final StandardRepositoryRecord rec1 = new StandardRepositoryRecord(queue);
            rec1.setWorking(flowFile1);
            rec1.setDestination(queue);

            // Create a Record that we can swap out
            final FlowFileRecord flowFile2 = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-111111111112")
                    .contentClaim(claim2)
                    .build();

            final StandardRepositoryRecord rec2 = new StandardRepositoryRecord(queue);
            rec2.setWorking(flowFile2);
            rec2.setDestination(queue);

            final List<RepositoryRecord> records = new ArrayList<>();
            records.add(rec1);
            records.add(rec2);
            repo.updateRepository(records);

            final String swapLocation = swapMgr.swapOut(Collections.singletonList(flowFile2), queue);
            repo.swapFlowFilesOut(Collections.singletonList(flowFile2), queue, swapLocation);
        }

        final ResourceClaimManager recoveryClaimManager = new StandardResourceClaimManager();
        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(NiFiProperties.createBasicNiFiProperties(null, null))) {
            repo.initialize(recoveryClaimManager);
            final long largestId = repo.loadFlowFiles(queueProvider, 0L);

            // largest ID known is 1 because this doesn't take into account the FlowFiles that have been swapped out
            assertEquals(1, largestId);
        }

        // resource claim 1 will have a single claimant count while resource claim 2 will have no claimant counts
        // because resource claim 2 is referenced only by flowfiles that are swapped out.
        assertEquals(1, recoveryClaimManager.getClaimantCount(resourceClaim1));
        assertEquals(0, recoveryClaimManager.getClaimantCount(resourceClaim2));

        final SwapSummary summary = queue.recoverSwappedFlowFiles();
        assertNotNull(summary);
        assertEquals(2, summary.getMaxFlowFileId().intValue());
        assertEquals(new QueueSize(1, 0L), summary.getQueueSize());

        final List<ResourceClaim> swappedOutClaims = summary.getResourceClaims();
        assertNotNull(swappedOutClaims);
        assertEquals(1, swappedOutClaims.size());
        assertEquals(claim2.getResourceClaim(), swappedOutClaims.get(0));
    }

    @Test
    public void testRestartWithOneRecord() throws IOException {
        final Path path = Paths.get("target/test-repo");
        if (Files.exists(path)) {
            FileUtils.deleteFile(path.toFile(), true);
        }

        final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(NiFiProperties.createBasicNiFiProperties(null, null));
        repo.initialize(new StandardResourceClaimManager());

        final TestQueueProvider queueProvider = new TestQueueProvider();
        repo.loadFlowFiles(queueProvider, 0L);

        final List<FlowFileRecord> flowFileCollection = new ArrayList<>();

        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                flowFileCollection.add((FlowFileRecord) invocation.getArguments()[0]);
                return null;
            }
        }).when(queue).put(any(FlowFileRecord.class));

        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);

        StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        ffBuilder.id(1L);
        ffBuilder.addAttribute("abc", "xyz");
        ffBuilder.size(0L);
        final FlowFileRecord flowFileRecord = ffBuilder.build();

        final List<RepositoryRecord> records = new ArrayList<>();
        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
        record.setWorking(flowFileRecord);
        record.setDestination(connection.getFlowFileQueue());
        records.add(record);

        repo.updateRepository(records);

        // update to add new attribute
        ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(flowFileRecord).addAttribute("hello", "world");
        final FlowFileRecord flowFileRecord2 = ffBuilder.build();
        record.setWorking(flowFileRecord2);
        repo.updateRepository(records);

        // update size but no attribute
        ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(flowFileRecord2).size(40L);
        final FlowFileRecord flowFileRecord3 = ffBuilder.build();
        record.setWorking(flowFileRecord3);
        repo.updateRepository(records);

        repo.close();

        // restore
        final WriteAheadFlowFileRepository repo2 = new WriteAheadFlowFileRepository(NiFiProperties.createBasicNiFiProperties(null, null));
        repo2.initialize(new StandardResourceClaimManager());
        repo2.loadFlowFiles(queueProvider, 0L);

        assertEquals(1, flowFileCollection.size());
        final FlowFileRecord flowFile = flowFileCollection.get(0);
        assertEquals(1L, flowFile.getId());
        assertEquals("xyz", flowFile.getAttribute("abc"));
        assertEquals(40L, flowFile.getSize());
        assertEquals("world", flowFile.getAttribute("hello"));

        repo2.close();
    }

    private static class TestQueueProvider implements QueueProvider {

        private List<Connection> connectionList = new ArrayList<>();

        public void addConnection(final Connection connection) {
            this.connectionList.add(connection);
        }

        @Override
        public Collection<FlowFileQueue> getAllQueues() {
            final List<FlowFileQueue> queueList = new ArrayList<>();
            for (final Connection conn : connectionList) {
                queueList.add(conn.getFlowFileQueue());
            }

            return queueList;
        }
    }

    private static class MockFlowFileSwapManager implements FlowFileSwapManager {

        private final Map<FlowFileQueue, Map<String, List<FlowFileRecord>>> swappedRecords = new HashMap<>();

        @Override
        public void initialize(SwapManagerInitializationContext initializationContext) {
        }

        @Override
        public String swapOut(List<FlowFileRecord> flowFiles, FlowFileQueue flowFileQueue) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                swapMap = new HashMap<>();
                swappedRecords.put(flowFileQueue, swapMap);
            }

            final String location = UUID.randomUUID().toString();
            swapMap.put(location, new ArrayList<>(flowFiles));
            return location;
        }

        @Override
        public SwapContents peek(String swapLocation, FlowFileQueue flowFileQueue) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            final List<FlowFileRecord> flowFiles = swapMap.get(swapLocation);
            final SwapSummary summary = getSwapSummary(swapLocation);
            return new StandardSwapContents(summary, flowFiles);
        }

        @Override
        public SwapContents swapIn(String swapLocation, FlowFileQueue flowFileQueue) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            final List<FlowFileRecord> flowFiles = swapMap.remove(swapLocation);
            final SwapSummary summary = getSwapSummary(swapLocation);
            return new StandardSwapContents(summary, flowFiles);
        }

        @Override
        public List<String> recoverSwapLocations(FlowFileQueue flowFileQueue) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            return new ArrayList<>(swapMap.keySet());
        }

        @Override
        public SwapSummary getSwapSummary(String swapLocation) throws IOException {
            List<FlowFileRecord> records = null;
            for (final Map<String, List<FlowFileRecord>> swapMap : swappedRecords.values()) {
                records = swapMap.get(swapLocation);
                if (records != null) {
                    break;
                }
            }

            if (records == null) {
                return null;
            }

            final List<ResourceClaim> resourceClaims = new ArrayList<>();
            long size = 0L;
            Long maxId = null;
            for (final FlowFileRecord flowFile : records) {
                size += flowFile.getSize();

                if (maxId == null || flowFile.getId() > maxId) {
                    maxId = flowFile.getId();
                }

                final ContentClaim contentClaim = flowFile.getContentClaim();
                if (contentClaim != null) {
                    final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
                    resourceClaims.add(resourceClaim);
                }
            }

            return new StandardSwapSummary(new QueueSize(records.size(), size), maxId, resourceClaims);
        }

        @Override
        public void purge() {
            this.swappedRecords.clear();
        }

    }
}
