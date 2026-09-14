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

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SpillableContentRepositoryTest {
    private static final long BUDGET = 100L;

    private ResourceClaimManager resourceClaimManager;
    private InMemoryContentRepository backingRepository;
    private FlowFileRepository flowFileRepository;
    private SpillableContentRepository repository;

    @BeforeEach
    void setup() throws IOException {
        resourceClaimManager = new StandardResourceClaimManager();
        backingRepository = new InMemoryContentRepository(resourceClaimManager);
        flowFileRepository = mock(FlowFileRepository.class);
        repository = new SpillableContentRepository(backingRepository, flowFileRepository, BUDGET);
        repository.initialize(new StandardContentRepositoryContext(resourceClaimManager, EventReporter.NO_OP));
    }

    @Test
    void testContentUnderBudgetStaysInMemory() throws IOException {
        final byte[] content = bytes(50);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        assertTrue(repository.isHeldInMemory(claim));
        assertFalse(repository.isSpilled(claim));
        assertFalse(repository.isAccessible(claim));
        assertEquals(50L, repository.size(claim));
        assertEquals(50L, repository.getInMemoryByteCount());
        assertArrayEquals(content, readAll(claim));
    }

    @Test
    void testInMemoryContentSupportsRepeatedReads() throws IOException {
        final SpillableContentRepository chunkedRepository = new SpillableContentRepository(backingRepository, flowFileRepository, 4096L);
        chunkedRepository.initialize(new StandardContentRepositoryContext(resourceClaimManager, EventReporter.NO_OP));
        final byte[] content = bytes(3000);
        final ContentClaim claim = chunkedRepository.create(false);
        try (final OutputStream out = chunkedRepository.write(claim)) {
            out.write(content);
        }

        assertArrayEquals(content, readAll(chunkedRepository, claim));
        assertArrayEquals(content, readAll(chunkedRepository, claim));
        assertEquals(content.length, chunkedRepository.getInMemoryByteCount());
    }

    @Test
    void testSingleWriteExceedingBudgetSpills() throws IOException {
        final byte[] content = bytes(150);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        assertTrue(repository.isSpilled(claim));
        assertFalse(repository.isHeldInMemory(claim));
        assertTrue(repository.isAccessible(claim));
        assertEquals(150L, repository.size(claim));
        assertEquals(0L, repository.getInMemoryByteCount());
        assertArrayEquals(content, readAll(claim));
    }

    @Test
    void testMultipleWritesCrossingBudgetSpillMidStream() throws IOException {
        final byte[] first = bytes(60);
        final byte[] second = bytes(60);
        final ContentClaim claim = repository.create(false);
        try (final OutputStream out = repository.write(claim)) {
            out.write(first);
            out.write(second);
        }

        assertTrue(repository.isSpilled(claim));
        assertEquals(0L, repository.getInMemoryByteCount());
        assertEquals(120L, repository.size(claim));

        final byte[] expected = new byte[120];
        System.arraycopy(first, 0, expected, 0, 60);
        System.arraycopy(second, 0, expected, 60, 60);
        assertArrayEquals(expected, readAll(claim));
    }

    @Test
    void testRemoveInMemoryClaimFreesMemory() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(50));
        assertEquals(50L, repository.getInMemoryByteCount());

        repository.remove(claim);
        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @Test
    void testClaimantCounts() throws IOException {
        final ContentClaim claim = repository.create(false);
        assertEquals(1, repository.getClaimantCount(claim));

        assertEquals(2, repository.incrementClaimaintCount(claim));
        assertEquals(1, repository.decrementClaimantCount(claim));
        assertEquals(0, repository.decrementClaimantCount(claim));
    }

    @Test
    void testPurgePreservesReferencedClaims() throws IOException {
        final byte[] inMemoryContent = bytes(50);
        final ContentClaim inMemoryClaim = repository.create(false);
        write(inMemoryClaim, inMemoryContent);

        final byte[] spilledContent = bytes(150);
        final ContentClaim spilledClaim = repository.create(false);
        write(spilledClaim, spilledContent);

        repository.purge();

        assertEquals(50L, repository.getInMemoryByteCount());
        assertArrayEquals(inMemoryContent, readAll(inMemoryClaim));
        assertArrayEquals(spilledContent, readAll(spilledClaim));
        verify(flowFileRepository, never()).updateRepository(anyList());
    }

    @Test
    void testPurgeWhileClaimIsBeingCreated() throws Exception {
        final ResourceClaimManager blockingResourceClaimManager = spy(new StandardResourceClaimManager());
        final CountDownLatch incrementStarted = new CountDownLatch(1);
        final CountDownLatch allowIncrement = new CountDownLatch(1);
        doAnswer(invocation -> {
            incrementStarted.countDown();
            if (!allowIncrement.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to increment claimant count");
            }
            return invocation.callRealMethod();
        }).when(blockingResourceClaimManager).incrementClaimantCount(any(ResourceClaim.class));

        final SpillableContentRepository concurrentRepository = new SpillableContentRepository(
            new InMemoryContentRepository(blockingResourceClaimManager), flowFileRepository, BUDGET);
        concurrentRepository.initialize(new StandardContentRepositoryContext(blockingResourceClaimManager, EventReporter.NO_OP));

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<ContentClaim> claimFuture = executorService.submit(() -> concurrentRepository.create(false));
            assertTrue(incrementStarted.await(10, TimeUnit.SECONDS));
            concurrentRepository.purge();
            allowIncrement.countDown();

            final ContentClaim claim = claimFuture.get(10, TimeUnit.SECONDS);
            try (final OutputStream out = concurrentRepository.write(claim)) {
                out.write(bytes(10));
            }
            assertArrayEquals(bytes(10), readAll(concurrentRepository, claim));
        } finally {
            allowIncrement.countDown();
            executorService.shutdownNow();
        }
    }

    @Test
    void testPurgeDiscardsUnreferencedInMemoryClaim() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(50));
        repository.decrementClaimantCount(claim);

        repository.purge();

        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @Test
    void testRemoveAfterPurgeDoesNotMakeMemoryCountNegative() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(50));
        repository.decrementClaimantCount(claim);
        repository.purge();

        repository.remove(claim);

        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @Test
    void testPurgePreservesClaimWithOpenWriter() throws IOException {
        final byte[] content = bytes(50);
        final ContentClaim claim = repository.create(false);
        try (final OutputStream out = repository.write(claim)) {
            out.write(content);
            repository.decrementClaimantCount(claim);
            repository.purge();
            assertEquals(content.length, repository.getInMemoryByteCount());
        }

        assertArrayEquals(content, readAll(claim));
        repository.purge();
        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @Test
    void testExportInMemoryClaim() throws IOException {
        final byte[] content = bytes(50);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        final ContentClaim exported = repository.exportForExternalUse(claim);
        assertNotSame(claim, exported);
        assertArrayEquals(content, readAll(exported));
        assertEquals(0L, repository.getInMemoryByteCount());

        assertEquals(1, resourceClaimManager.getClaimantCount(exported.getResourceClaim()));
        repository.commitExportForExternalUse(claim);
        assertEquals(0, resourceClaimManager.getClaimantCount(exported.getResourceClaim()));
    }

    @Test
    void testExportInMemoryClaimIsIdempotent() throws IOException {
        final byte[] content = bytes(50);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        final ContentClaim firstExport = repository.exportForExternalUse(claim);
        final ContentClaim secondExport = repository.exportForExternalUse(claim);

        assertSame(firstExport, secondExport);
        assertArrayEquals(content, readAll(secondExport));
        assertEquals(1, resourceClaimManager.getClaimantCount(firstExport.getResourceClaim()));

        repository.commitExportForExternalUse(claim);
        repository.commitExportForExternalUse(claim);
        assertEquals(0, resourceClaimManager.getClaimantCount(firstExport.getResourceClaim()));
    }

    @Test
    void testExportEmptyInMemoryClaimClosesBackingStream() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, new byte[0]);

        final ContentClaim exported = repository.exportForExternalUse(claim);

        assertEquals(1, backingRepository.getWriteCount());
        assertEquals(1, backingRepository.getClosedStreamCount());
        assertEquals(0L, exported.getLength());
        repository.commitExportForExternalUse(claim);
    }

    @Test
    void testExportFailureSubmitsBackingClaimForCleanup() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(50));
        backingRepository.failNextWrite();

        assertThrows(IOException.class, () -> repository.exportForExternalUse(claim));

        repository.purge();
        verify(flowFileRepository).updateRepository(anyList());
    }

    @Test
    void testPurgeCleansExportWhenExternalUpdateFails() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(50));

        final ContentClaim exportedClaim = repository.exportForExternalUse(claim);
        repository.incrementClaimaintCount(exportedClaim);
        repository.decrementClaimantCount(exportedClaim);
        repository.decrementClaimantCount(claim);
        repository.purge();

        final ArgumentCaptor<List<RepositoryRecord>> recordsCaptor = captureRepositoryRecords();
        verify(flowFileRepository).updateRepository(recordsCaptor.capture());
        assertEquals(List.of(exportedClaim), recordsCaptor.getValue().getFirst().getTransientClaims());
        assertEquals(0, resourceClaimManager.getClaimantCount(exportedClaim.getResourceClaim()));
    }

    @Test
    void testExportSpilledClaimHandsOffBackingClaim() throws IOException {
        final byte[] content = bytes(150);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        final ContentClaim exported = repository.exportForExternalUse(claim);
        assertArrayEquals(content, readAll(exported));

        assertEquals(1, resourceClaimManager.getClaimantCount(exported.getResourceClaim()));
        repository.commitExportForExternalUse(claim);
        assertEquals(0, resourceClaimManager.getClaimantCount(exported.getResourceClaim()));

        repository.purge();
        verify(flowFileRepository, never()).updateRepository(anyList());
    }

    @Test
    void testPurgeSubmitsUnexportedSpilledClaims() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(150));
        final ContentClaim unrelatedClaim = repository.create(false);
        final ContentClaim backingClaim = repository.exportForExternalUse(unrelatedClaim);
        repository.commitExportForExternalUse(unrelatedClaim);
        repository.decrementClaimantCount(claim);

        assertTrue(repository.isSpilled(claim));

        repository.purge();

        assertEquals(0L, repository.getInMemoryByteCount());

        final ArgumentCaptor<List<RepositoryRecord>> captor = captureRepositoryRecords();
        verify(flowFileRepository).updateRepository(captor.capture());
        final List<RepositoryRecord> records = captor.getValue();
        assertEquals(1, records.size());
        final RepositoryRecord record = records.get(0);
        assertEquals(RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS, record.getType());
        assertEquals(1, record.getTransientClaims().size());

        assertFalse(record.getTransientClaims().contains(backingClaim));
    }

    @Test
    void testPurgeRetriesFailedCleanupSubmission() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(150));
        repository.decrementClaimantCount(claim);
        doThrow(new IOException("First update failed")).doNothing().when(flowFileRepository).updateRepository(anyList());

        repository.purge();
        repository.purge();

        verify(flowFileRepository, times(2)).updateRepository(anyList());
    }

    @Test
    void testPurgeCleansOnlyUnreferencedSpilledClaim() throws IOException {
        final ContentClaim unreferencedClaim = repository.create(false);
        write(unreferencedClaim, bytes(150));
        repository.decrementClaimantCount(unreferencedClaim);

        final byte[] referencedContent = bytes(160);
        final ContentClaim referencedClaim = repository.create(false);
        write(referencedClaim, referencedContent);

        repository.purge();

        final ArgumentCaptor<List<RepositoryRecord>> recordsCaptor = ArgumentCaptor.forClass(List.class);
        verify(flowFileRepository).updateRepository(recordsCaptor.capture());
        assertEquals(1, recordsCaptor.getValue().getFirst().getTransientClaims().size());
        assertArrayEquals(referencedContent, readAll(referencedClaim));
    }

    @Test
    void testSpillFailureRetainsBufferedContentAndMemoryAccounting() throws IOException {
        final byte[] content = bytes(60);
        final ContentClaim claim = repository.create(false);
        final OutputStream out = repository.write(claim);
        out.write(content);
        backingRepository.failNextWrite();

        assertThrows(IOException.class, () -> out.write(bytes(60)));
        out.close();

        assertEquals(60L, repository.getInMemoryByteCount());
        assertArrayEquals(content, readAll(claim));
    }

    @Test
    void testSpilledResourceClaimCanBeRead() throws IOException {
        final byte[] content = bytes(150);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        assertEquals(content.length, repository.size(claim.getResourceClaim()));
        try (final InputStream in = repository.read(claim.getResourceClaim())) {
            assertArrayEquals(content, in.readAllBytes());
        }
    }

    @Test
    void testExportOverwritesDestination(@TempDir final Path tempDirectory) throws IOException {
        final byte[] content = bytes(50);
        final ContentClaim claim = repository.create(false);
        write(claim, content);
        final Path destination = tempDirectory.resolve("content");
        Files.write(destination, bytes(100));

        repository.exportTo(claim, destination, false);

        assertArrayEquals(content, Files.readAllBytes(destination));
    }

    @Test
    void testClaimAllowsOnlyOneWriter() throws IOException {
        final ContentClaim claim = repository.create(false);
        final OutputStream out = repository.write(claim);

        assertThrows(IllegalStateException.class, () -> repository.write(claim));
        out.close();
        assertThrows(IllegalStateException.class, () -> repository.write(claim));
    }

    @Test
    void testDeferredRepositoryResolvesChangedThreshold() throws IOException {
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final AtomicLong threshold = new AtomicLong(BUDGET);
        when(processGroup.resolveStatelessContentMaxHeap()).thenAnswer(invocation -> threshold.get());
        final DeferredStatelessContentRepository deferredRepository = new DeferredStatelessContentRepository(
            processGroup, backingRepository, flowFileRepository, resourceClaimManager, EventReporter.NO_OP);

        final ContentClaim inMemoryClaim = deferredRepository.create(false);
        assertEquals("in-memory", inMemoryClaim.getResourceClaim().getContainer());
        deferredRepository.decrementClaimantCount(inMemoryClaim);
        deferredRepository.purge();

        threshold.set(0L);
        final ContentClaim backingClaim = deferredRepository.create(false);
        assertEquals("container", backingClaim.getResourceClaim().getContainer());
    }

    @Test
    void testCloneAcrossStates() throws IOException {
        final byte[] smallContent = bytes(40);
        final ContentClaim small = repository.create(false);
        write(small, smallContent);
        final ContentClaim smallClone = repository.clone(small, false);
        assertArrayEquals(smallContent, readAll(smallClone));
        assertTrue(repository.isHeldInMemory(smallClone));

        final byte[] largeContent = bytes(150);
        final ContentClaim large = repository.create(false);
        write(large, largeContent);
        final ContentClaim largeClone = repository.clone(large, false);
        assertArrayEquals(largeContent, readAll(largeClone));
        assertTrue(repository.isSpilled(largeClone));
    }

    @Test
    void testImportFromAndExportTo() throws IOException {
        final byte[] content = bytes(150);
        final ContentClaim claim = repository.create(false);
        final long imported = repository.importFrom(new ByteArrayInputStream(content), claim);
        assertEquals(150L, imported);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        repository.exportTo(claim, out);
        assertArrayEquals(content, out.toByteArray());
    }

    private ArgumentCaptor<List<RepositoryRecord>> captureRepositoryRecords() {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<RepositoryRecord>> captor = ArgumentCaptor.forClass(List.class);
        return captor;
    }

    private void write(final ContentClaim claim, final byte[] content) throws IOException {
        try (final OutputStream out = repository.write(claim)) {
            out.write(content);
        }
    }

    private byte[] readAll(final ContentClaim claim) throws IOException {
        return readAll(repository, claim);
    }

    private byte[] readAll(final ContentRepository contentRepository, final ContentClaim claim) throws IOException {
        try (final InputStream in = contentRepository.read(claim)) {
            return in.readAllBytes();
        }
    }

    private static byte[] bytes(final int length) {
        final byte[] content = new byte[length];
        for (int i = 0; i < length; i++) {
            content[i] = (byte) ('A' + (i % 26));
        }

        return content;
    }

    /**
     * A minimal on-heap {@link ContentRepository} used to stand in for the NiFi Content Repository that a {@link SpillableContentRepository} spills to. It shares
     * the {@link ResourceClaimManager} used by the repository under test so that claimant counts can be asserted uniformly.
     */
    private static final class InMemoryContentRepository implements ContentRepository {
        private final ResourceClaimManager resourceClaimManager;
        private final Map<ResourceClaim, byte[]> contents = new HashMap<>();
        private final AtomicInteger idGenerator = new AtomicInteger(0);
        private final AtomicInteger writeCount = new AtomicInteger(0);
        private final AtomicInteger closedStreamCount = new AtomicInteger(0);
        private boolean failNextWrite;

        private InMemoryContentRepository(final ResourceClaimManager resourceClaimManager) {
            this.resourceClaimManager = resourceClaimManager;
        }

        @Override
        public void initialize(final ContentRepositoryContext context) {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public Set<String> getContainerNames() {
            return Set.of("container");
        }

        @Override
        public long getContainerCapacity(final String containerName) {
            return 0;
        }

        @Override
        public long getContainerUsableSpace(final String containerName) {
            return 0;
        }

        @Override
        public String getContainerFileStoreName(final String containerName) {
            return "container";
        }

        @Override
        public ContentClaim create(final boolean lossTolerant) {
            final ResourceClaim resourceClaim = new StandardResourceClaim(resourceClaimManager, "container", "section", "backing-" + idGenerator.getAndIncrement(), lossTolerant);
            final StandardContentClaim contentClaim = new StandardContentClaim(resourceClaim, 0L);
            contentClaim.setLength(0L);
            resourceClaimManager.incrementClaimantCount(resourceClaim);
            return contentClaim;
        }

        @Override
        public int incrementClaimaintCount(final ContentClaim claim) {
            return resourceClaimManager.incrementClaimantCount(claim.getResourceClaim());
        }

        @Override
        public int getClaimantCount(final ContentClaim claim) {
            return resourceClaimManager.getClaimantCount(claim.getResourceClaim());
        }

        @Override
        public int decrementClaimantCount(final ContentClaim claim) {
            return resourceClaimManager.decrementClaimantCount(claim.getResourceClaim());
        }

        @Override
        public boolean remove(final ContentClaim claim) {
            contents.remove(claim.getResourceClaim());
            return true;
        }

        @Override
        public ContentClaim clone(final ContentClaim original, final boolean lossTolerant) throws IOException {
            final ContentClaim clone = create(lossTolerant);
            try (final InputStream in = read(original);
                 final OutputStream out = write(clone)) {
                in.transferTo(out);
            }

            return clone;
        }

        @Override
        public long importFrom(final Path content, final ContentClaim claim) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long importFrom(final InputStream content, final ContentClaim claim) throws IOException {
            try (final OutputStream out = write(claim)) {
                return content.transferTo(out);
            }
        }

        @Override
        public long exportTo(final ContentClaim claim, final Path destination, final boolean append) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long exportTo(final ContentClaim claim, final Path destination, final boolean append, final long offset, final long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long exportTo(final ContentClaim claim, final OutputStream destination) throws IOException {
            try (final InputStream in = read(claim)) {
                return in.transferTo(destination);
            }
        }

        @Override
        public long exportTo(final ContentClaim claim, final OutputStream destination, final long offset, final long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size(final ContentClaim claim) {
            return claim.getLength();
        }

        @Override
        public long size(final ResourceClaim claim) {
            final byte[] data = contents.get(claim);
            return data == null ? 0 : data.length;
        }

        @Override
        public InputStream read(final ContentClaim claim) {
            final byte[] data = contents.getOrDefault(claim.getResourceClaim(), new byte[0]);
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream read(final ResourceClaim claim) {
            return new ByteArrayInputStream(contents.getOrDefault(claim, new byte[0]));
        }

        @Override
        public OutputStream write(final ContentClaim claim) {
            final StandardContentClaim standardContentClaim = (StandardContentClaim) claim;
            writeCount.incrementAndGet();
            if (failNextWrite) {
                failNextWrite = false;
                return new OutputStream() {
                    @Override
                    public void write(final int value) throws IOException {
                        throw new IOException("Write failed");
                    }

                    @Override
                    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
                        throw new IOException("Write failed");
                    }

                    @Override
                    public void close() {
                        closedStreamCount.incrementAndGet();
                    }
                };
            }

            return new ByteArrayOutputStream() {
                @Override
                public void close() {
                    final byte[] data = toByteArray();
                    contents.put(standardContentClaim.getResourceClaim(), data);
                    standardContentClaim.setLength(data.length);
                    closedStreamCount.incrementAndGet();
                }
            };
        }

        private void failNextWrite() {
            failNextWrite = true;
        }

        private int getWriteCount() {
            return writeCount.get();
        }

        private int getClosedStreamCount() {
            return closedStreamCount.get();
        }

        @Override
        public void purge() {
            contents.clear();
        }

        @Override
        public void cleanup() {
        }

        @Override
        public boolean isAccessible(final ContentClaim contentClaim) {
            return contentClaim != null && contents.containsKey(contentClaim.getResourceClaim());
        }
    }
}
