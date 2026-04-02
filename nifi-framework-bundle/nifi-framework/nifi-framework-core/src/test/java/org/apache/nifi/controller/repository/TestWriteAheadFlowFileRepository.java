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

import org.apache.nifi.components.connector.DropFlowFileSummary;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueueSize;
import org.apache.nifi.controller.queue.ListFlowFileStatus;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.PollStrategy;
import org.apache.nifi.controller.queue.QueueDiagnostics;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.StandardFlowFileQueue;
import org.apache.nifi.controller.queue.StandardLocalQueuePartitionDiagnostics;
import org.apache.nifi.controller.queue.StandardQueueDiagnostics;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.controller.swap.StandardSwapContents;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.wali.SequentialAccessWriteAheadLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.WriteAheadRepository;

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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestWriteAheadFlowFileRepository {
    private static final Logger logger = LoggerFactory.getLogger(TestWriteAheadFlowFileRepository.class);
    private static NiFiProperties niFiProperties;

    @BeforeEach
    public void setUp() throws Exception {
        niFiProperties = NiFiProperties.createBasicNiFiProperties(TestWriteAheadFlowFileRepository.class.getResource("/conf/nifi.properties").getFile());
        clearRepo();
    }

    @AfterEach
    public void tearDown() throws Exception {
        clearRepo();
    }

    public void clearRepo() throws IOException {
        final File target = new File("target");
        final File testRepo = new File(target, "test-repo");
        if (testRepo.exists()) {
            FileUtils.deleteFile(testRepo, true);
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "nifi.test.performance", matches = "true",
            disabledReason = "Intended only for local performance testing before/after making changes")
    public void testUpdatePerformance() throws IOException, InterruptedException {
        final FlowFileQueue queue = new FlowFileQueue() {
            private LoadBalanceCompression compression = LoadBalanceCompression.DO_NOT_COMPRESS;

            @Override
            public void startLoadBalancing() {
            }

            @Override
            public void stopLoadBalancing() {
            }

            @Override
            public void offloadQueue() {
            }

            @Override
            public void resetOffloadedQueue() {
            }

            @Override
            public boolean isActivelyLoadBalancing() {
                return false;
            }

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
            public long getTotalQueuedDuration(long fromTimestamp) {
                return 0;
            }

            @Override
            public long getMinLastQueueDate() {
                return 0;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public FlowFileAvailability getFlowFileAvailability() {
                return isActiveQueueEmpty() ? FlowFileAvailability.ACTIVE_QUEUE_EMPTY : FlowFileAvailability.FLOWFILE_AVAILABLE;
            }

            @Override
            public boolean isActiveQueueEmpty() {
                return false;
            }

            @Override
            public void acknowledge(FlowFileRecord flowFile) {
            }

            @Override
            public void acknowledge(Collection<FlowFileRecord> flowFiles) {
            }

            @Override
            public boolean isUnacknowledgedFlowFile() {
                return false;
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
            public FlowFileRecord poll(Set<FlowFileRecord> expiredRecords, PollStrategy pollStrategy) {
                return null;
            }

            @Override
            public FlowFileRecord poll(Set<FlowFileRecord> expiredRecords) {
                return null;
            }

            @Override
            public List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords, PollStrategy pollStrategy) {
                return null;
            }

            @Override
            public List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords) {
                return null;
            }

            @Override
            public List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords, PollStrategy pollStrategy) {
                return null;
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
            public long getFlowFileExpiration(TimeUnit timeUnit) {
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
            public DropFlowFileSummary dropFlowFiles(Predicate<FlowFile> predicate) {
                return new DropFlowFileSummary(0, 0L);
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
            public FlowFileRecord getFlowFile(String flowFileUuid) {
                return null;
            }

            @Override
            public void verifyCanList() throws IllegalStateException {
            }

            @Override
            public QueueDiagnostics getQueueDiagnostics() {
                final FlowFileQueueSize size = new FlowFileQueueSize(size().getObjectCount(), size().getByteCount(), 0, 0, 0, 0, 0);
                return new StandardQueueDiagnostics(new StandardLocalQueuePartitionDiagnostics(size, false, false, new QueueSize(0, 0), new QueueSize(0, 0)), Collections.emptyList());
            }

            @Override
            public void lock() {
            }

            @Override
            public void unlock() {
            }

            @Override
            public void setLoadBalanceStrategy(final LoadBalanceStrategy strategy, final String partitioningAttribute) {
            }

            @Override
            public LoadBalanceStrategy getLoadBalanceStrategy() {
                return null;
            }

            @Override
            public void setLoadBalanceCompression(final LoadBalanceCompression compression) {
                this.compression = compression;
            }

            @Override
            public LoadBalanceCompression getLoadBalanceCompression() {
                return compression;
            }

            @Override
            public String getPartitioningAttribute() {
                return null;
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
        final StandardRepositoryRecordSerdeFactory serdeFactory = new StandardRepositoryRecordSerdeFactory(claimManager);
        final WriteAheadRepository<SerializedRepositoryRecord> repo = new SequentialAccessWriteAheadLog<>(path.toFile(), serdeFactory);
        final Collection<SerializedRepositoryRecord> initialRecs = repo.recoverRecords();
        assertTrue(initialRecs.isEmpty());

        final int updateCountPerThread = totalUpdates / numThreads;

        final Thread[] threads = new Thread[numThreads];
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < numThreads; i++) {
                final Thread t = new Thread(() -> {
                    final List<SerializedRepositoryRecord> records = new ArrayList<>();
                    final int numBatches = updateCountPerThread / batchSize;
                    final MockFlowFile baseFlowFile = new MockFlowFile(0L);

                    for (int i1 = 0; i1 < numBatches; i1++) {
                        records.clear();
                        for (int k = 0; k < batchSize; k++) {
                            final MockFlowFileRecord flowFile = new MockFlowFileRecord(baseFlowFile.getAttributes(), baseFlowFile.getSize());
                            final String uuid = flowFile.getAttribute("uuid");

                            final StandardRepositoryRecord record = new StandardRepositoryRecord(null, flowFile);
                            record.setDestination(queue);
                            final Map<String, String> updatedAttrs = Collections.singletonMap("uuid", uuid);
                            record.setWorking(flowFile, updatedAttrs, false);

                            records.add(new LiveSerializedRepositoryRecord(record));
                        }

                        assertThrows(IOException.class, () -> repo.update(records, false));
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
                logger.info("{} ms to insert {} updates using {} partitions and {} threads, *as a warmup!*",
                        millis, updateCountPerThread * numThreads, numPartitions, numThreads);
            } else {
                logger.info("{} ms to insert {} updates using {} partitions and {} threads",
                        millis, updateCountPerThread * numThreads, numPartitions, numThreads);
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
    public void testNormalizeSwapLocation() {
        assertEquals("/", WriteAheadFlowFileRepository.normalizeSwapLocation("/"));
        assertEquals("", WriteAheadFlowFileRepository.normalizeSwapLocation(""));
        assertNull(WriteAheadFlowFileRepository.normalizeSwapLocation(null));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation("test.txt"));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation("/test.txt"));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation("/tmp/test.txt"));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation("//test.txt"));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation("/path/to/other/file/repository/test.txt"));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation("test.txt/"));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation("/path/to/test.txt/"));
    }

    @Test
    public void testSwapLocationsRestored() throws IOException {
        final Path path = Paths.get("target/test-swap-repo");
        if (Files.exists(path)) {
            FileUtils.deleteFile(path.toFile(), true);
        }

        final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties);
        repo.initialize(new StandardResourceClaimManager());

        final TestQueueProvider queueProvider = new TestQueueProvider();
        repo.loadFlowFiles(queueProvider);

        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);

        StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        ffBuilder.id(1L);
        ffBuilder.size(0L);
        final FlowFileRecord flowFileRecord = ffBuilder.build();

        final List<RepositoryRecord> records = new ArrayList<>();
        final StandardRepositoryRecord record = new StandardRepositoryRecord(queue, flowFileRecord, "swap123");
        record.setDestination(queue);
        records.add(record);

        repo.updateRepository(records);
        repo.close();

        // restore
        final WriteAheadFlowFileRepository repo2 = new WriteAheadFlowFileRepository(niFiProperties);
        repo2.initialize(new StandardResourceClaimManager());
        repo2.loadFlowFiles(queueProvider);
        assertTrue(repo2.isValidSwapLocationSuffix("swap123"));
        assertFalse(repo2.isValidSwapLocationSuffix("other"));
        repo2.close();
    }

    @Test
    public void testSwapLocationsUpdatedOnRepoUpdate() throws IOException {
        final Path path = Paths.get("target/test-swap-repo");
        if (Files.exists(path)) {
            FileUtils.deleteFile(path.toFile(), true);
        }

        final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties);
        repo.initialize(new StandardResourceClaimManager());

        final TestQueueProvider queueProvider = new TestQueueProvider();
        repo.loadFlowFiles(queueProvider);

        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);

        StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        ffBuilder.id(1L);
        ffBuilder.size(0L);
        final FlowFileRecord flowFileRecord = ffBuilder.build();

        final List<RepositoryRecord> records = new ArrayList<>();
        final StandardRepositoryRecord record = new StandardRepositoryRecord(queue, flowFileRecord, "/tmp/swap123");
        record.setDestination(queue);
        records.add(record);

        assertFalse(repo.isValidSwapLocationSuffix("swap123"));
        repo.updateRepository(records);
        assertTrue(repo.isValidSwapLocationSuffix("swap123"));

        repo.swapFlowFilesIn("/tmp/swap123", Collections.singletonList(flowFileRecord), queue);
        assertFalse(repo.isValidSwapLocationSuffix("swap123"));

        repo.close();
    }

    @Test
    public void testResourceClaimsIncremented() throws IOException {
        final ResourceClaimManager claimManager = new StandardResourceClaimManager();

        final TestQueueProvider queueProvider = new TestQueueProvider();
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");
        when(connection.getDestination()).thenReturn(Mockito.mock(Connectable.class));

        final FlowFileSwapManager swapMgr = new MockFlowFileSwapManager();
        final FlowFileQueue queue = new StandardFlowFileQueue("1234", null, null, null, swapMgr, null, 10000, "0 sec", 0L, "0 B");

        when(connection.getFlowFileQueue()).thenReturn(queue);
        queueProvider.addConnection(connection);

        final ResourceClaim resourceClaim1 = claimManager.newResourceClaim("container", "section", "1", false, false);
        final ContentClaim claim1 = new StandardContentClaim(resourceClaim1, 0L);

        final ResourceClaim resourceClaim2 = claimManager.newResourceClaim("container", "section", "2", false, false);
        final ContentClaim claim2 = new StandardContentClaim(resourceClaim2, 0L);

        // Create a flowfile repo, update it once with a FlowFile that points to one resource claim. Then,
        // indicate that a FlowFile was swapped out. We should then be able to recover these FlowFiles and the
        // resource claims' counts should be updated for both the swapped out FlowFile and the non-swapped out FlowFile
        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(claimManager);
            repo.loadFlowFiles(queueProvider);

            // Create a Repository Record that indicates that a FlowFile was created
            final FlowFileRecord flowFile1 = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-111111111111")
                    .contentClaim(claim1)
                    .build();
            final StandardRepositoryRecord rec1 = new StandardRepositoryRecord(queue);
            rec1.setWorking(flowFile1, false);
            rec1.setDestination(queue);

            // Create a Record that we can swap out
            final FlowFileRecord flowFile2 = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-111111111112")
                    .contentClaim(claim2)
                    .build();

            final StandardRepositoryRecord rec2 = new StandardRepositoryRecord(queue);
            rec2.setWorking(flowFile2, true);
            rec2.setDestination(queue);

            final List<RepositoryRecord> records = new ArrayList<>();
            records.add(rec1);
            records.add(rec2);
            repo.updateRepository(records);

            final String swapLocation = swapMgr.swapOut(Collections.singletonList(flowFile2), queue, null);
            repo.swapFlowFilesOut(Collections.singletonList(flowFile2), queue, swapLocation);
        }

        final ResourceClaimManager recoveryClaimManager = new StandardResourceClaimManager();
        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(recoveryClaimManager);
            final long largestId = repo.loadFlowFiles(queueProvider);

            // largest ID known is 1 because this doesn't take into account the FlowFiles that have been swapped out
            assertEquals(1, largestId);
        }

        // resource claim 1 will have a single claimant count while resource claim 2 will have no claimant counts
        // because resource claim 2 is referenced only by FlowFiles that are swapped out.
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

        final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties);
        repo.initialize(new StandardResourceClaimManager());

        final TestQueueProvider queueProvider = new TestQueueProvider();
        repo.loadFlowFiles(queueProvider);

        final List<FlowFileRecord> flowFileCollection = new ArrayList<>();

        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        doAnswer((Answer<Object>) invocation -> {
            flowFileCollection.add((FlowFileRecord) invocation.getArguments()[0]);
            return null;
        }).when(queue).put(any(FlowFileRecord.class));

        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);

        StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        ffBuilder.id(1L);
        ffBuilder.addAttribute("abc", "xyz");
        ffBuilder.size(0L);
        final FlowFileRecord flowFileRecord = ffBuilder.build();

        final List<RepositoryRecord> records = new ArrayList<>();
        final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null);
        record.setWorking(flowFileRecord, false);
        record.setDestination(connection.getFlowFileQueue());
        records.add(record);

        repo.updateRepository(records);

        // update to add new attribute
        ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(flowFileRecord).addAttribute("hello", "world");
        final FlowFileRecord flowFileRecord2 = ffBuilder.build();
        record.setWorking(flowFileRecord2, false);
        repo.updateRepository(records);

        // update size but no attribute
        ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(flowFileRecord2).size(40L);
        final FlowFileRecord flowFileRecord3 = ffBuilder.build();
        record.setWorking(flowFileRecord3, true);
        repo.updateRepository(records);

        repo.close();

        // restore
        final WriteAheadFlowFileRepository repo2 = new WriteAheadFlowFileRepository(niFiProperties);
        repo2.initialize(new StandardResourceClaimManager());
        repo2.loadFlowFiles(queueProvider);

        assertEquals(1, flowFileCollection.size());
        final FlowFileRecord flowFile = flowFileCollection.get(0);
        assertEquals(1L, flowFile.getId());
        assertEquals("xyz", flowFile.getAttribute("abc"));
        assertEquals(40L, flowFile.getSize());
        assertEquals("world", flowFile.getAttribute("hello"));

        repo2.close();
    }

    @Test
    public void testSelectiveDropWithSwappedFlowFilesAndRecovery() throws IOException {
        final Path path = Paths.get("target/test-repo");
        if (Files.exists(path)) {
            FileUtils.deleteFile(path.toFile(), true);
        }

        final ResourceClaimManager claimManager = new StandardResourceClaimManager();
        final TestQueueProvider queueProvider = new TestQueueProvider();
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("selective-drop-test");
        when(connection.getDestination()).thenReturn(Mockito.mock(Connectable.class));

        final MockFlowFileSwapManager swapMgr = new MockFlowFileSwapManager();
        final FlowFileQueue queue = new StandardFlowFileQueue("selective-drop-test", null, null, null, swapMgr, null, 10000, "0 sec", 0L, "0 B");
        when(connection.getFlowFileQueue()).thenReturn(queue);
        queueProvider.addConnection(connection);

        final List<FlowFileRecord> allFlowFiles = new ArrayList<>();
        final String originalSwapLocation;
        final String newSwapLocation;

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(claimManager);
            repo.loadFlowFiles(queueProvider);

            for (int i = 0; i < 20; i++) {
                final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                        .id(i + 1)
                        .addAttribute("uuid", UUID.randomUUID().toString())
                        .addAttribute("index", String.valueOf(i))
                        .size(i)
                        .build();

                final StandardRepositoryRecord record = new StandardRepositoryRecord(queue);
                record.setWorking(flowFile, false);
                record.setDestination(queue);
                repo.updateRepository(Collections.singletonList(record));
                allFlowFiles.add(flowFile);
            }

            final List<FlowFileRecord> flowFilesToSwap = new ArrayList<>();
            for (int i = 10; i < 20; i++) {
                flowFilesToSwap.add(allFlowFiles.get(i));
            }
            originalSwapLocation = swapMgr.swapOut(flowFilesToSwap, queue, null);
            repo.swapFlowFilesOut(flowFilesToSwap, queue, originalSwapLocation);

            final List<RepositoryRecord> selectiveDropRecords = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                final FlowFileRecord ff = allFlowFiles.get(i);
                final StandardRepositoryRecord dropRecord = new StandardRepositoryRecord(queue, ff);
                dropRecord.markForDelete();
                selectiveDropRecords.add(dropRecord);
            }

            for (int i = 10; i < 15; i++) {
                final FlowFileRecord ff = allFlowFiles.get(i);
                final StandardRepositoryRecord dropRecord = new StandardRepositoryRecord(queue, ff);
                dropRecord.markForDelete();
                selectiveDropRecords.add(dropRecord);
            }

            final List<FlowFileRecord> remainingSwapped = new ArrayList<>();
            for (int i = 15; i < 20; i++) {
                remainingSwapped.add(allFlowFiles.get(i));
            }
            newSwapLocation = swapMgr.swapOut(remainingSwapped, queue, null);

            for (final FlowFileRecord ff : remainingSwapped) {
                final StandardRepositoryRecord swapRecord = new StandardRepositoryRecord(queue, ff, newSwapLocation);
                swapRecord.setDestination(queue);
                selectiveDropRecords.add(swapRecord);
            }

            final StandardRepositoryRecord swapFileRenamedRecord = new StandardRepositoryRecord(queue);
            swapFileRenamedRecord.setSwapFileRenamed(originalSwapLocation, newSwapLocation);
            selectiveDropRecords.add(swapFileRenamedRecord);

            repo.updateRepository(selectiveDropRecords);

            swapMgr.deleteSwapFile(originalSwapLocation);
        }

        final List<FlowFileRecord> recoveredFlowFiles = new ArrayList<>();
        final ResourceClaimManager recoveryClaimManager = new StandardResourceClaimManager();

        final Connection recoveryConnection = Mockito.mock(Connection.class);
        when(recoveryConnection.getIdentifier()).thenReturn("selective-drop-test");
        when(recoveryConnection.getDestination()).thenReturn(Mockito.mock(Connectable.class));

        final FlowFileQueue recoveryQueue = Mockito.mock(FlowFileQueue.class);
        when(recoveryQueue.getIdentifier()).thenReturn("selective-drop-test");
        doAnswer(invocation -> {
            recoveredFlowFiles.add((FlowFileRecord) invocation.getArguments()[0]);
            return null;
        }).when(recoveryQueue).put(any(FlowFileRecord.class));

        when(recoveryConnection.getFlowFileQueue()).thenReturn(recoveryQueue);

        final TestQueueProvider recoveryQueueProvider = new TestQueueProvider();
        recoveryQueueProvider.addConnection(recoveryConnection);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(recoveryClaimManager);
            repo.loadFlowFiles(recoveryQueueProvider);

            assertEquals(5, recoveredFlowFiles.size());

            for (final FlowFileRecord ff : recoveredFlowFiles) {
                final int index = Integer.parseInt(ff.getAttribute("index"));
                assertTrue(index >= 5 && index < 10, "Expected active FlowFiles with index 5-9, but found index " + index);
            }

            assertFalse(repo.isValidSwapLocationSuffix(originalSwapLocation), "Original swap location should not be valid after selective drop");
            assertTrue(repo.isValidSwapLocationSuffix(newSwapLocation), "New swap location should be valid after selective drop");
        }
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
        public String swapOut(List<FlowFileRecord> flowFiles, FlowFileQueue flowFileQueue, final String partitionName) {
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
        public SwapContents peek(String swapLocation, FlowFileQueue flowFileQueue) {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            final List<FlowFileRecord> flowFiles = swapMap.get(swapLocation);
            final SwapSummary summary = getSwapSummary(swapLocation);
            return new StandardSwapContents(summary, flowFiles);
        }

        @Override
        public SwapContents swapIn(String swapLocation, FlowFileQueue flowFileQueue) {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            final List<FlowFileRecord> flowFiles = swapMap.remove(swapLocation);
            final SwapSummary summary = getSwapSummary(swapLocation);
            return new StandardSwapContents(summary, flowFiles);
        }

        @Override
        public List<String> recoverSwapLocations(FlowFileQueue flowFileQueue, final String partitionName) {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            return new ArrayList<>(swapMap.keySet());
        }

        @Override
        public SwapSummary getSwapSummary(String swapLocation) {
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

            return new StandardSwapSummary(new QueueSize(records.size(), size), maxId, resourceClaims, 0L, 0L);
        }

        @Override
        public void purge() {
            this.swappedRecords.clear();
        }

        @Override
        public void deleteSwapFile(final String swapLocation) {
            for (final Map<String, List<FlowFileRecord>> swapMap : swappedRecords.values()) {
                swapMap.remove(swapLocation);
            }
        }

        @Override
        public String getQueueIdentifier(final String swapLocation) {
            return null;
        }

        @Override
        public Set<String> getSwappedPartitionNames(FlowFileQueue queue) {
            return Collections.emptySet();
        }

        @Override
        public String changePartitionName(String swapLocation, String newPartitionName) {
            return swapLocation;
        }
    }

    // =========================================================================
    // Truncation Feature: Helpers
    // =========================================================================

    /**
     * Creates a mock queue + connection + queueProvider wired together, suitable for runtime truncation tests.
     * Returns [claimManager, queueProvider, queue].
     */
    private record RuntimeRepoContext(StandardResourceClaimManager claimManager, TestQueueProvider queueProvider, FlowFileQueue queue) {
    }

    private RuntimeRepoContext createRuntimeRepoContext() {
        final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();
        final TestQueueProvider queueProvider = new TestQueueProvider();
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");
        when(connection.getDestination()).thenReturn(Mockito.mock(Connectable.class));
        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        when(connection.getFlowFileQueue()).thenReturn(queue);
        queueProvider.addConnection(connection);
        return new RuntimeRepoContext(claimManager, queueProvider, queue);
    }

    private StandardContentClaim createClaim(final ResourceClaim rc, final long offset, final long length, final boolean truncationCandidate) {
        final StandardContentClaim claim = new StandardContentClaim(rc, offset);
        claim.setLength(length);
        if (truncationCandidate) {
            claim.setTruncationCandidate(true);
        }
        return claim;
    }

    private void createAndDeleteFlowFile(final WriteAheadFlowFileRepository repo, final FlowFileQueue queue,
                                         final ContentClaim claim) throws IOException {
        final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", UUID.randomUUID().toString())
                .contentClaim(claim)
                .build();

        final StandardRepositoryRecord createRecord = new StandardRepositoryRecord(queue);
        createRecord.setWorking(flowFile, false);
        createRecord.setDestination(queue);
        repo.updateRepository(List.of(createRecord));

        final StandardRepositoryRecord deleteRecord = new StandardRepositoryRecord(queue, flowFile);
        deleteRecord.markForDelete();
        repo.updateRepository(List.of(deleteRecord));
    }

    /**
     * Writes FlowFiles (one per claim) to a new repo, closes it, then recovers into a fresh repo
     * and returns the recovered FlowFileRecords.
     */
    private List<FlowFileRecord> writeAndRecover(final ContentClaim... claims) throws IOException {
        final ResourceClaimManager writeClaimManager = new StandardResourceClaimManager();
        final TestQueueProvider writeQueueProvider = new TestQueueProvider();
        final Connection writeConnection = Mockito.mock(Connection.class);
        when(writeConnection.getIdentifier()).thenReturn("1234");
        when(writeConnection.getDestination()).thenReturn(Mockito.mock(Connectable.class));
        final FlowFileSwapManager swapMgr = new MockFlowFileSwapManager();
        final FlowFileQueue writeQueue = new StandardFlowFileQueue("1234", null, null, null, swapMgr, null, 10000, "0 sec", 0L, "0 B");
        when(writeConnection.getFlowFileQueue()).thenReturn(writeQueue);
        writeQueueProvider.addConnection(writeConnection);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(writeClaimManager);
            repo.loadFlowFiles(writeQueueProvider);

            final List<RepositoryRecord> records = new ArrayList<>();
            for (int i = 0; i < claims.length; i++) {
                final FlowFileRecord ff = new StandardFlowFileRecord.Builder()
                        .id(i + 1L)
                        .addAttribute("uuid", "11111111-1111-1111-1111-" + String.format("%012d", i + 1))
                        .contentClaim(claims[i])
                        .build();
                final StandardRepositoryRecord rec = new StandardRepositoryRecord(writeQueue);
                rec.setWorking(ff, false);
                rec.setDestination(writeQueue);
                records.add(rec);
            }
            repo.updateRepository(records);
        }

        // Recover
        final List<FlowFileRecord> recovered = new ArrayList<>();
        final FlowFileQueue recoveryQueue = Mockito.mock(FlowFileQueue.class);
        when(recoveryQueue.getIdentifier()).thenReturn("1234");
        doAnswer(invocation -> {
            recovered.add((FlowFileRecord) invocation.getArguments()[0]);
            return null;
        }).when(recoveryQueue).put(any(FlowFileRecord.class));

        final Connection recoveryConnection = Mockito.mock(Connection.class);
        when(recoveryConnection.getIdentifier()).thenReturn("1234");
        when(recoveryConnection.getFlowFileQueue()).thenReturn(recoveryQueue);
        final TestQueueProvider recoveryQueueProvider = new TestQueueProvider();
        recoveryQueueProvider.addConnection(recoveryConnection);

        try (final WriteAheadFlowFileRepository repo2 = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo2.initialize(new StandardResourceClaimManager());
            repo2.loadFlowFiles(recoveryQueueProvider);
        }

        return recovered;
    }

    private FlowFileRecord findRecoveredByOffset(final List<FlowFileRecord> recovered, final long offset) {
        return recovered.stream()
                .filter(ff -> ff.getContentClaim() != null && ff.getContentClaim().getOffset() == offset)
                .findFirst()
                .orElse(null);
    }

    // =========================================================================
    // Truncation Feature: Runtime Tests
    // =========================================================================

    @Test
    public void testDeleteRecordRoutesTruncatableClaimToTruncationQueue() throws IOException {
        final RuntimeRepoContext context = createRuntimeRepoContext();
        final ResourceClaim resourceClaim = context.claimManager().newResourceClaim("container", "section", "1", false, false);
        context.claimManager().incrementClaimantCount(resourceClaim);
        context.claimManager().incrementClaimantCount(resourceClaim); // count = 2 so that after delete decrement it stays > 0 (not destructable)
        final StandardContentClaim contentClaim = createClaim(resourceClaim, 1024L, 5_000_000L, true);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(context.claimManager());
            repo.loadFlowFiles(context.queueProvider());
            createAndDeleteFlowFile(repo, context.queue(), contentClaim);
            repo.checkpoint();
        }

        final List<ContentClaim> truncated = new ArrayList<>();
        context.claimManager().drainTruncatableClaims(truncated, 100);
        assertTrue(truncated.contains(contentClaim));
    }

    @Test
    public void testDestructableClaimTakesPriorityOverTruncatable() throws IOException {
        final RuntimeRepoContext context = createRuntimeRepoContext();
        final ResourceClaim resourceClaim = context.claimManager().newResourceClaim("container", "section", "1", false, false);
        context.claimManager().incrementClaimantCount(resourceClaim); // count = 1 -- will reach 0 after delete
        final StandardContentClaim contentClaim = createClaim(resourceClaim, 1024L, 5_000_000L, true);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(context.claimManager());
            repo.loadFlowFiles(context.queueProvider());
            createAndDeleteFlowFile(repo, context.queue(), contentClaim);
            repo.checkpoint();
        }

        final List<ResourceClaim> destructed = new ArrayList<>();
        context.claimManager().drainDestructableClaims(destructed, 100);
        assertTrue(destructed.contains(resourceClaim));

        final List<ContentClaim> truncated = new ArrayList<>();
        context.claimManager().drainTruncatableClaims(truncated, 100);
        assertFalse(truncated.contains(contentClaim));
    }

    @Test
    public void testUpdateRecordOriginalClaimQueuedForTruncation() throws IOException {
        final RuntimeRepoContext context = createRuntimeRepoContext();

        final ResourceClaim originalResourceClaim = context.claimManager().newResourceClaim("container", "section", "1", false, false);
        context.claimManager().incrementClaimantCount(originalResourceClaim);
        context.claimManager().incrementClaimantCount(originalResourceClaim); // count = 2 so it stays > 0 after decrement
        final StandardContentClaim originalClaim = createClaim(originalResourceClaim, 2048L, 5_000_000L, true);

        final ResourceClaim newResourceClaim = context.claimManager().newResourceClaim("container", "section", "2", false, false);
        context.claimManager().incrementClaimantCount(newResourceClaim);
        final StandardContentClaim newClaim = createClaim(newResourceClaim, 0L, 100L, false);

        final FlowFileRecord originalFlowFile = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", UUID.randomUUID().toString())
                .contentClaim(originalClaim)
                .build();

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(context.claimManager());
            repo.loadFlowFiles(context.queueProvider());

            final StandardRepositoryRecord createRecord = new StandardRepositoryRecord(context.queue());
            createRecord.setWorking(originalFlowFile, false);
            createRecord.setDestination(context.queue());
            repo.updateRepository(List.of(createRecord));

            final FlowFileRecord updatedFlowFile = new StandardFlowFileRecord.Builder()
                    .fromFlowFile(originalFlowFile)
                    .contentClaim(newClaim)
                    .build();
            final StandardRepositoryRecord updateRecord = new StandardRepositoryRecord(context.queue(), originalFlowFile);
            updateRecord.setWorking(updatedFlowFile, true);
            updateRecord.setDestination(context.queue());
            repo.updateRepository(List.of(updateRecord));
            repo.checkpoint();
        }

        final List<ContentClaim> truncated = new ArrayList<>();
        context.claimManager().drainTruncatableClaims(truncated, 100);
        assertTrue(truncated.contains(originalClaim));
    }

    // =========================================================================
    // Truncation Feature: Recovery Tests
    // =========================================================================

    @Test
    public void testRecoveryMarksTruncationCandidateForLargeTailClaim() throws IOException {
        final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", "1", false, false);
        final StandardContentClaim smallClaim = createClaim(resourceClaim, 0L, 100L, false);
        final StandardContentClaim largeClaim = createClaim(resourceClaim, 100L, 2_000_000L, false);

        final List<FlowFileRecord> recovered = writeAndRecover(smallClaim, largeClaim);

        final FlowFileRecord recoveredLargeFlowFile = findRecoveredByOffset(recovered, 100L);
        assertNotNull(recoveredLargeFlowFile);
        assertTrue(recoveredLargeFlowFile.getContentClaim().isTruncationCandidate());

        final FlowFileRecord recoveredSmallFlowFile = findRecoveredByOffset(recovered, 0L);
        assertNotNull(recoveredSmallFlowFile);
        assertFalse(recoveredSmallFlowFile.getContentClaim().isTruncationCandidate());
    }

    @Test
    public void testRecoveryMarksClonedClaimButRefCountPreventsTruncation() throws IOException {
        final StandardResourceClaimManager writeClaimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = writeClaimManager.newResourceClaim("container", "section", "1", false, false);
        final StandardContentClaim smallClaim = createClaim(resourceClaim, 0L, 100L, false);
        final StandardContentClaim sharedClaim = createClaim(resourceClaim, 100L, 2_000_000L, false);

        // Write three FlowFiles: one small (to keep the ResourceClaim alive) and two sharing the
        // large claim (clone scenario). Then close the repo.
        final TestQueueProvider writeQueueProvider = new TestQueueProvider();
        final Connection writeConnection = Mockito.mock(Connection.class);
        when(writeConnection.getIdentifier()).thenReturn("1234");
        when(writeConnection.getDestination()).thenReturn(Mockito.mock(Connectable.class));
        final FlowFileSwapManager swapManager = new MockFlowFileSwapManager();
        final FlowFileQueue writeQueue = new StandardFlowFileQueue("1234", null, null, null, swapManager, null, 10000, "0 sec", 0L, "0 B");
        when(writeConnection.getFlowFileQueue()).thenReturn(writeQueue);
        writeQueueProvider.addConnection(writeConnection);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(writeClaimManager);
            repo.loadFlowFiles(writeQueueProvider);

            final FlowFileRecord smallFlowFile = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-000000000001")
                    .contentClaim(smallClaim)
                    .build();
            final StandardRepositoryRecord createSmall = new StandardRepositoryRecord(writeQueue);
            createSmall.setWorking(smallFlowFile, false);
            createSmall.setDestination(writeQueue);

            final List<RepositoryRecord> records = new ArrayList<>();
            records.add(createSmall);
            for (int i = 0; i < 2; i++) {
                final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                        .id(i + 2L)
                        .addAttribute("uuid", "11111111-1111-1111-1111-" + String.format("%012d", i + 2))
                        .contentClaim(sharedClaim)
                        .build();
                final StandardRepositoryRecord record = new StandardRepositoryRecord(writeQueue);
                record.setWorking(flowFile, false);
                record.setDestination(writeQueue);
                records.add(record);
            }
            repo.updateRepository(records);
        }

        // Recover into a new repo and verify that the shared claim is marked as a truncation
        // candidate (since it is the tail claim) and the reference count reflects both FlowFiles.
        final List<FlowFileRecord> recovered = new ArrayList<>();
        final FlowFileQueue recoveryQueue = Mockito.mock(FlowFileQueue.class);
        when(recoveryQueue.getIdentifier()).thenReturn("1234");
        doAnswer(invocation -> {
            recovered.add((FlowFileRecord) invocation.getArguments()[0]);
            return null;
        }).when(recoveryQueue).put(any(FlowFileRecord.class));

        final Connection recoveryConnection = Mockito.mock(Connection.class);
        when(recoveryConnection.getIdentifier()).thenReturn("1234");
        when(recoveryConnection.getFlowFileQueue()).thenReturn(recoveryQueue);
        final TestQueueProvider recoveryQueueProvider = new TestQueueProvider();
        recoveryQueueProvider.addConnection(recoveryConnection);

        final StandardResourceClaimManager recoveryClaimManager = new StandardResourceClaimManager();
        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(recoveryClaimManager);
            repo.loadFlowFiles(recoveryQueueProvider);

            assertEquals(3, recovered.size());
            final FlowFileRecord firstSharedFlowFile = recovered.stream().filter(flowFile -> flowFile.getContentClaim().getOffset() == 100L).findFirst().orElseThrow();
            final FlowFileRecord secondSharedFlowFile = recovered.stream()
                .filter(flowFile -> flowFile.getContentClaim().getOffset() == 100L && flowFile != firstSharedFlowFile)
                .findFirst()
                .orElseThrow();
            final ContentClaim recoveredClaim = firstSharedFlowFile.getContentClaim();
            assertTrue(recoveredClaim.isTruncationCandidate());
            assertEquals(2, repo.getContentClaimReferenceCount(recoveredClaim));

            // Delete first FlowFile with the shared claim — should not be truncatable (second still references it)
            final StandardRepositoryRecord deleteFirstSharedRecord = new StandardRepositoryRecord(recoveryQueue, firstSharedFlowFile);
            deleteFirstSharedRecord.markForDelete();
            repo.updateRepository(List.of(deleteFirstSharedRecord));
            repo.checkpoint();

            List<ContentClaim> truncated = new ArrayList<>();
            recoveryClaimManager.drainTruncatableClaims(truncated, 100);
            assertFalse(truncated.contains(recoveredClaim), "Shared claim should not be truncatable while another FlowFile still references it");

            // Delete second FlowFile with the shared claim — should now be truncatable because
            // the small FlowFile keeps the ResourceClaim alive but no FlowFile references the large ContentClaim
            final StandardRepositoryRecord deleteSecondSharedRecord = new StandardRepositoryRecord(recoveryQueue, secondSharedFlowFile);
            deleteSecondSharedRecord.markForDelete();
            repo.updateRepository(List.of(deleteSecondSharedRecord));
            repo.checkpoint();

            truncated = new ArrayList<>();
            recoveryClaimManager.drainTruncatableClaims(truncated, 100);
            assertTrue(truncated.contains(recoveredClaim), "Claim should be truncatable after all referencing FlowFiles are removed");
        }
    }

    @Test
    public void testRecoveryOnlyMarksTailClaim() throws IOException {
        final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", "1", false, false);
        final StandardContentClaim nonTailClaim = createClaim(resourceClaim, 100L, 2_000_000L, false);
        final StandardContentClaim tailClaim = createClaim(resourceClaim, 2_000_100L, 3_000_000L, false);

        final List<FlowFileRecord> recovered = writeAndRecover(nonTailClaim, tailClaim);

        final FlowFileRecord tailFlowFile = findRecoveredByOffset(recovered, 2_000_100L);
        assertNotNull(tailFlowFile);
        assertTrue(tailFlowFile.getContentClaim().isTruncationCandidate());

        final FlowFileRecord nonTailFlowFile = findRecoveredByOffset(recovered, 100L);
        assertNotNull(nonTailFlowFile);
        assertFalse(nonTailFlowFile.getContentClaim().isTruncationCandidate());
    }

    @Test
    public void testRecoverySmallClaimAfterLargeDoesNotMarkLarge() throws IOException {
        final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", "1", false, false);
        final StandardContentClaim firstSmallClaim = createClaim(resourceClaim, 0L, 100L, false);
        final StandardContentClaim largeClaim = createClaim(resourceClaim, 100L, 2_000_000L, false);
        final StandardContentClaim secondSmallClaim = createClaim(resourceClaim, 2_000_100L, 50L, false);

        final List<FlowFileRecord> recovered = writeAndRecover(firstSmallClaim, largeClaim, secondSmallClaim);

        final List<FlowFileRecord> flowFilesWithClaims = recovered.stream()
                .filter(flowFile -> flowFile.getContentClaim() != null)
                .toList();

        assertFalse(flowFilesWithClaims.isEmpty());
        for (final FlowFileRecord flowFile : flowFilesWithClaims) {
            assertFalse(flowFile.getContentClaim().isTruncationCandidate(),
                    "No claim should be a truncation candidate because the large claim is not the tail; claim offset=" + flowFile.getContentClaim().getOffset());
        }
    }

    @Test
    public void testSharedClaimNotTruncatedUntilAllReferencesRemoved() throws IOException {
        final RuntimeRepoContext context = createRuntimeRepoContext();
        final ResourceClaim resourceClaim = context.claimManager().newResourceClaim("container", "section", "1", false, false);
        context.claimManager().incrementClaimantCount(resourceClaim);
        context.claimManager().incrementClaimantCount(resourceClaim);
        context.claimManager().incrementClaimantCount(resourceClaim);

        final StandardContentClaim smallClaim = createClaim(resourceClaim, 0L, 100L, false);
        final StandardContentClaim largeClaim = createClaim(resourceClaim, 100L, 5_000_000L, true);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(context.claimManager());
            repo.loadFlowFiles(context.queueProvider());

            final FlowFileRecord smallFlowFile = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(smallClaim)
                    .build();
            final StandardRepositoryRecord createSmallRecord = new StandardRepositoryRecord(context.queue());
            createSmallRecord.setWorking(smallFlowFile, false);
            createSmallRecord.setDestination(context.queue());
            repo.updateRepository(List.of(createSmallRecord));

            final FlowFileRecord firstLargeFlowFile = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(largeClaim)
                    .build();
            final StandardRepositoryRecord createFirstLargeRecord = new StandardRepositoryRecord(context.queue());
            createFirstLargeRecord.setWorking(firstLargeFlowFile, false);
            createFirstLargeRecord.setDestination(context.queue());
            repo.updateRepository(List.of(createFirstLargeRecord));

            final FlowFileRecord secondLargeFlowFile = new StandardFlowFileRecord.Builder()
                    .id(3L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(largeClaim)
                    .build();
            final StandardRepositoryRecord createSecondLargeRecord = new StandardRepositoryRecord(context.queue());
            createSecondLargeRecord.setWorking(secondLargeFlowFile, false);
            createSecondLargeRecord.setDestination(context.queue());
            repo.updateRepository(List.of(createSecondLargeRecord));

            // Delete first FlowFile referencing the large shared claim
            final StandardRepositoryRecord deleteFirstLargeRecord = new StandardRepositoryRecord(context.queue(), firstLargeFlowFile);
            deleteFirstLargeRecord.markForDelete();
            repo.updateRepository(List.of(deleteFirstLargeRecord));
            repo.checkpoint();

            List<ContentClaim> truncated = new ArrayList<>();
            context.claimManager().drainTruncatableClaims(truncated, 100);
            assertFalse(truncated.contains(largeClaim),
                    "Shared claim should not be truncatable while another FlowFile still references it");

            // Delete remaining FlowFile referencing the large shared claim
            final StandardRepositoryRecord deleteSecondLargeRecord = new StandardRepositoryRecord(context.queue(), secondLargeFlowFile);
            deleteSecondLargeRecord.markForDelete();
            repo.updateRepository(List.of(deleteSecondLargeRecord));
            repo.checkpoint();

            truncated = new ArrayList<>();
            context.claimManager().drainTruncatableClaims(truncated, 100);
            assertTrue(truncated.contains(largeClaim),
                    "Claim should be truncatable after all referencing FlowFiles are removed");
        }
    }

    @Test
    public void testNonSharedClaimTruncatedOnDelete() throws IOException {
        final RuntimeRepoContext context = createRuntimeRepoContext();
        final ResourceClaim resourceClaim = context.claimManager().newResourceClaim("container", "section", "1", false, false);
        context.claimManager().incrementClaimantCount(resourceClaim);
        context.claimManager().incrementClaimantCount(resourceClaim);

        final StandardContentClaim smallClaim = createClaim(resourceClaim, 0L, 100L, false);
        final StandardContentClaim largeClaim = createClaim(resourceClaim, 100L, 5_000_000L, true);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(context.claimManager());
            repo.loadFlowFiles(context.queueProvider());

            final FlowFileRecord smallFlowFile = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(smallClaim)
                    .build();
            final StandardRepositoryRecord createSmallRecord = new StandardRepositoryRecord(context.queue());
            createSmallRecord.setWorking(smallFlowFile, false);
            createSmallRecord.setDestination(context.queue());
            repo.updateRepository(List.of(createSmallRecord));

            final FlowFileRecord largeFlowFile = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(largeClaim)
                    .build();
            final StandardRepositoryRecord createLargeRecord = new StandardRepositoryRecord(context.queue());
            createLargeRecord.setWorking(largeFlowFile, false);
            createLargeRecord.setDestination(context.queue());
            repo.updateRepository(List.of(createLargeRecord));

            final StandardRepositoryRecord deleteLargeRecord = new StandardRepositoryRecord(context.queue(), largeFlowFile);
            deleteLargeRecord.markForDelete();
            repo.updateRepository(List.of(deleteLargeRecord));
            repo.checkpoint();

            final List<ContentClaim> truncated = new ArrayList<>();
            context.claimManager().drainTruncatableClaims(truncated, 100);
            assertTrue(truncated.contains(largeClaim),
                    "Non-shared claim should be truncatable after the sole referencing FlowFile is deleted");
        }
    }

    @Test
    public void testRecoverySkipsTruncationCandidatesWhenSwapLocationsExist() throws IOException {
        final StandardResourceClaimManager writeClaimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = writeClaimManager.newResourceClaim("container", "section", "1", false, false);
        final StandardContentClaim smallClaim = createClaim(resourceClaim, 0L, 100L, false);
        final StandardContentClaim largeClaim = createClaim(resourceClaim, 100L, 2_000_000L, false);

        final TestQueueProvider writeQueueProvider = new TestQueueProvider();
        final Connection writeConnection = Mockito.mock(Connection.class);
        when(writeConnection.getIdentifier()).thenReturn("1234");
        when(writeConnection.getDestination()).thenReturn(Mockito.mock(Connectable.class));
        final FlowFileSwapManager swapManager = new MockFlowFileSwapManager();
        final FlowFileQueue writeQueue = new StandardFlowFileQueue("1234", null, null, null, swapManager, null, 10000, "0 sec", 0L, "0 B");
        when(writeConnection.getFlowFileQueue()).thenReturn(writeQueue);
        writeQueueProvider.addConnection(writeConnection);

        // Write two FlowFiles, then swap one out so the WAL records a swap location.
        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(writeClaimManager);
            repo.loadFlowFiles(writeQueueProvider);

            final FlowFileRecord smallFlowFile = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-000000000001")
                    .contentClaim(smallClaim)
                    .build();
            final StandardRepositoryRecord createSmall = new StandardRepositoryRecord(writeQueue);
            createSmall.setWorking(smallFlowFile, false);
            createSmall.setDestination(writeQueue);

            final FlowFileRecord largeFlowFile = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-000000000002")
                    .contentClaim(largeClaim)
                    .build();
            final StandardRepositoryRecord createLarge = new StandardRepositoryRecord(writeQueue);
            createLarge.setWorking(largeFlowFile, false);
            createLarge.setDestination(writeQueue);

            repo.updateRepository(List.of(createSmall, createLarge));

            final StandardRepositoryRecord swapOut = new StandardRepositoryRecord(writeQueue, largeFlowFile, "swap-location-1");
            swapOut.setDestination(writeQueue);
            repo.updateRepository(List.of(swapOut));
        }

        // Recover into a new repo and verify that no claims are marked as truncation candidates.
        final List<FlowFileRecord> recovered = new ArrayList<>();
        final FlowFileQueue recoveryQueue = Mockito.mock(FlowFileQueue.class);
        when(recoveryQueue.getIdentifier()).thenReturn("1234");
        doAnswer(invocation -> {
            recovered.add((FlowFileRecord) invocation.getArguments()[0]);
            return null;
        }).when(recoveryQueue).put(any(FlowFileRecord.class));

        final Connection recoveryConnection = Mockito.mock(Connection.class);
        when(recoveryConnection.getIdentifier()).thenReturn("1234");
        when(recoveryConnection.getFlowFileQueue()).thenReturn(recoveryQueue);
        final TestQueueProvider recoveryQueueProvider = new TestQueueProvider();
        recoveryQueueProvider.addConnection(recoveryConnection);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(recoveryQueueProvider);
        }

        // The swapped-out FlowFile is excluded from wal.recoverRecords(), so only the small
        // FlowFile is recovered. The large claim at the tail would normally be a truncation
        // candidate, but the presence of swap locations must suppress all candidate marking.
        for (final FlowFileRecord flowFile : recovered) {
            if (flowFile.getContentClaim() != null) {
                assertFalse(flowFile.getContentClaim().isTruncationCandidate(),
                        "No claim should be a truncation candidate when swap files exist at recovery time");
            }
        }
    }

    @Test
    public void testTruncationDisabledPreventsClaimsFromBeingTruncatable() throws IOException {
        final NiFiProperties disabledTruncationProperties = NiFiProperties.createBasicNiFiProperties(
                TestWriteAheadFlowFileRepository.class.getResource("/conf/nifi.properties").getFile(),
                Map.of(NiFiProperties.CONTENT_CLAIM_TRUNCATION_ENABLED, "false"));

        final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();
        final TestQueueProvider queueProvider = new TestQueueProvider();
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");
        when(connection.getDestination()).thenReturn(Mockito.mock(Connectable.class));
        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        when(connection.getFlowFileQueue()).thenReturn(queue);
        queueProvider.addConnection(connection);

        final ResourceClaim resourceClaim = claimManager.newResourceClaim("container", "section", "1", false, false);
        claimManager.incrementClaimantCount(resourceClaim);
        claimManager.incrementClaimantCount(resourceClaim);
        final StandardContentClaim smallClaim = createClaim(resourceClaim, 0L, 100L, false);
        final StandardContentClaim largeClaim = createClaim(resourceClaim, 100L, 5_000_000L, true);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(disabledTruncationProperties)) {
            repo.initialize(claimManager);
            repo.loadFlowFiles(queueProvider);

            final FlowFileRecord smallFlowFile = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(smallClaim)
                    .build();
            final StandardRepositoryRecord createSmall = new StandardRepositoryRecord(queue);
            createSmall.setWorking(smallFlowFile, false);
            createSmall.setDestination(queue);
            repo.updateRepository(List.of(createSmall));

            final FlowFileRecord largeFlowFile = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(largeClaim)
                    .build();
            final StandardRepositoryRecord createLarge = new StandardRepositoryRecord(queue);
            createLarge.setWorking(largeFlowFile, false);
            createLarge.setDestination(queue);
            repo.updateRepository(List.of(createLarge));

            final StandardRepositoryRecord deleteLarge = new StandardRepositoryRecord(queue, largeFlowFile);
            deleteLarge.markForDelete();
            repo.updateRepository(List.of(deleteLarge));
            repo.checkpoint();

            final List<ContentClaim> truncated = new ArrayList<>();
            claimManager.drainTruncatableClaims(truncated, 100);
            assertTrue(truncated.isEmpty(), "No claims should be truncatable when truncation is disabled");
        }
    }

    @Test
    public void testOnSyncRecheckPreventsStaleTruncation() throws IOException {
        final RuntimeRepoContext context = createRuntimeRepoContext();
        final ResourceClaim resourceClaim = context.claimManager().newResourceClaim("container", "section", "1", false, false);
        context.claimManager().incrementClaimantCount(resourceClaim);
        context.claimManager().incrementClaimantCount(resourceClaim);
        final StandardContentClaim contentClaim = createClaim(resourceClaim, 1024L, 5_000_000L, true);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(context.claimManager());
            repo.loadFlowFiles(context.queueProvider());

            // Create and then delete a FlowFile — this queues the claim for truncation in claimsAwaitingTruncation
            createAndDeleteFlowFile(repo, context.queue(), contentClaim);

            // Before checkpoint (which calls onSync/onGlobalSync), simulate a replay by creating
            // a new FlowFile referencing the same claim. This increments the truncation reference count.
            final FlowFileRecord replayedFlowFile = new StandardFlowFileRecord.Builder()
                    .id(100L)
                    .addAttribute("uuid", UUID.randomUUID().toString())
                    .contentClaim(contentClaim)
                    .build();
            final StandardRepositoryRecord replayCreate = new StandardRepositoryRecord(context.queue());
            replayCreate.setWorking(replayedFlowFile, false);
            replayCreate.setDestination(context.queue());
            repo.updateRepository(List.of(replayCreate));

            // Checkpoint triggers onSync/onGlobalSync which drains claimsAwaitingTruncation.
            // The isTruncationAllowed re-check should detect the positive ref count and skip markTruncatable.
            repo.checkpoint();

            final List<ContentClaim> truncated = new ArrayList<>();
            context.claimManager().drainTruncatableClaims(truncated, 100);
            assertFalse(truncated.contains(contentClaim),
                    "Claim should not be truncatable because a replayed FlowFile still references it");
        }
    }

    @Test
    public void testTruncationReferenceCountCleanupAfterManyCreateDeleteCycles() throws IOException {
        final RuntimeRepoContext context = createRuntimeRepoContext();
        final ResourceClaim resourceClaim = context.claimManager().newResourceClaim("container", "section", "1", false, false);
        context.claimManager().incrementClaimantCount(resourceClaim);
        context.claimManager().incrementClaimantCount(resourceClaim);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(context.claimManager());
            repo.loadFlowFiles(context.queueProvider());

            for (int i = 0; i < 50; i++) {
                final StandardContentClaim claim = createClaim(resourceClaim, (i + 1) * 1024L, 5_000_000L, true);
                createAndDeleteFlowFile(repo, context.queue(), claim);
                assertEquals(0, repo.getContentClaimReferenceCount(claim),
                        "Reference count should be 0 after FlowFile is deleted for claim at offset " + claim.getOffset());
            }
        }
    }

    @Test
    public void testRecoveryPropagatesTruncationCandidateToAllInstances() throws IOException {
        final ResourceClaimManager writeClaimManager = new StandardResourceClaimManager();
        final TestQueueProvider writeQueueProvider = new TestQueueProvider();
        final Connection writeConnection = Mockito.mock(Connection.class);
        when(writeConnection.getIdentifier()).thenReturn("1234");
        when(writeConnection.getDestination()).thenReturn(Mockito.mock(Connectable.class));
        final FlowFileSwapManager swapManager = new MockFlowFileSwapManager();
        final FlowFileQueue writeQueue = new StandardFlowFileQueue("1234", null, null, null, swapManager, null, 10000, "0 sec", 0L, "0 B");
        when(writeConnection.getFlowFileQueue()).thenReturn(writeQueue);
        writeQueueProvider.addConnection(writeConnection);

        final ResourceClaim sharedResourceClaim = writeClaimManager.newResourceClaim("container", "section", "shared-1", false, false);

        // Two FlowFiles sharing the same large tail claim
        final StandardContentClaim largeClaim = createClaim(sharedResourceClaim, 100L, 5_000_000L, true);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(writeClaimManager);
            repo.loadFlowFiles(writeQueueProvider);

            final FlowFileRecord firstFlowFile = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-000000000001")
                    .contentClaim(largeClaim)
                    .build();
            final FlowFileRecord secondFlowFile = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-000000000002")
                    .contentClaim(largeClaim)
                    .build();

            final StandardRepositoryRecord firstCreateRecord = new StandardRepositoryRecord(writeQueue);
            firstCreateRecord.setWorking(firstFlowFile, false);
            firstCreateRecord.setDestination(writeQueue);
            final StandardRepositoryRecord secondCreateRecord = new StandardRepositoryRecord(writeQueue);
            secondCreateRecord.setWorking(secondFlowFile, false);
            secondCreateRecord.setDestination(writeQueue);
            repo.updateRepository(List.of(firstCreateRecord, secondCreateRecord));
        }

        // Recover into a new repo
        final List<FlowFileRecord> recovered = new ArrayList<>();
        final FlowFileQueue recoveryQueue = Mockito.mock(FlowFileQueue.class);
        when(recoveryQueue.getIdentifier()).thenReturn("1234");
        doAnswer(invocation -> {
            recovered.add((FlowFileRecord) invocation.getArguments()[0]);
            return null;
        }).when(recoveryQueue).put(any(FlowFileRecord.class));

        final Connection recoveryConnection = Mockito.mock(Connection.class);
        when(recoveryConnection.getIdentifier()).thenReturn("1234");
        when(recoveryConnection.getFlowFileQueue()).thenReturn(recoveryQueue);
        final TestQueueProvider recoveryQueueProvider = new TestQueueProvider();
        recoveryQueueProvider.addConnection(recoveryConnection);

        try (final WriteAheadFlowFileRepository repo = new WriteAheadFlowFileRepository(niFiProperties)) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(recoveryQueueProvider);
        }

        assertEquals(2, recovered.size());

        // Both recovered FlowFiles should have the truncationCandidate flag set, even though
        // they hold different StandardContentClaim object instances for the same logical claim.
        for (final FlowFileRecord flowFile : recovered) {
            assertNotNull(flowFile.getContentClaim());
            assertTrue(flowFile.getContentClaim().isTruncationCandidate(),
                    "All instances of the shared large tail claim should have truncationCandidate=true after recovery");
        }
    }
}
