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

import org.apache.nifi.controller.repository.SpillableContentRepository.MemoryContents;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.stateless.repository.ByteArrayContentRepository;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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
    private TrackingContentRepository backingRepository;
    private FlowFileRepository flowFileRepository;
    private SpillableContentRepository repository;

    @BeforeEach
    void setup() throws IOException {
        resourceClaimManager = new StandardResourceClaimManager();
        backingRepository = new TrackingContentRepository();
        backingRepository.initialize(new StandardContentRepositoryContext(resourceClaimManager, EventReporter.NO_OP));
        flowFileRepository = mock(FlowFileRepository.class);
        repository = new SpillableContentRepository(backingRepository, flowFileRepository, BUDGET);
        repository.initialize(new StandardContentRepositoryContext(resourceClaimManager, EventReporter.NO_OP));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 200_000})
    void testInMemoryContentSupportsRepeatedReads(final int length) throws IOException {
        repository = new SpillableContentRepository(backingRepository, flowFileRepository, Math.max(BUDGET, length));
        repository.initialize(new StandardContentRepositoryContext(resourceClaimManager, EventReporter.NO_OP));
        final byte[] content = bytes(length);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        assertTrue(repository.isHeldInMemory(claim));
        assertFalse(repository.isSpilled(claim));
        assertFalse(repository.isAccessible(claim));
        assertEquals(length, repository.size(claim));
        assertEquals(Math.max(32, length), repository.getInMemoryByteCount());
        assertArrayEquals(content, readAll(claim));
        assertArrayEquals(content, readAll(claim));
        final ContentClaim exported = repository.exportForExternalUse(claim);
        assertArrayEquals(content, readAll(exported));
        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @Test
    void testSmallClaimsAccountForAllocatedCapacity() throws Exception {
        long allocated = 0L;
        int spilled = 0;
        for (int claimIndex = 0; claimIndex < 100; claimIndex++) {
            final ContentClaim claim = repository.create(false);
            try (final OutputStream output = repository.write(claim)) {
                output.write(1);
            }

            if (repository.isHeldInMemory(claim)) {
                allocated += allocatedBytes(claim);
            } else {
                spilled++;
            }

            assertArrayEquals(new byte[] {1}, readAll(claim));
            assertEquals(allocated, repository.getInMemoryByteCount());
            assertTrue(allocated <= BUDGET);
        }

        assertTrue(spilled > 0);
    }

    @Test
    void testConcurrentWritersShareCapacityBudget() throws Exception {
        final List<Future<ContentClaim>> futures = new ArrayList<>();
        final CountDownLatch startWriting = new CountDownLatch(1);
        try (final ExecutorService executor = Executors.newFixedThreadPool(8)) {
            for (int claimIndex = 0; claimIndex < 32; claimIndex++) {
                futures.add(executor.submit(() -> {
                    final ContentClaim claim = repository.create(false);
                    assertTrue(startWriting.await(10, TimeUnit.SECONDS));
                    try (final OutputStream output = repository.write(claim)) {
                        output.write(bytes(32));
                    }

                    return claim;
                }));
            }

            startWriting.countDown();
            long allocated = 0L;
            for (final Future<ContentClaim> future : futures) {
                final ContentClaim claim = future.get(10, TimeUnit.SECONDS);
                assertArrayEquals(bytes(32), readAll(claim));
                if (repository.isHeldInMemory(claim)) {
                    allocated += allocatedBytes(claim);
                }

                repository.decrementClaimantCount(claim);
            }

            assertEquals(allocated, repository.getInMemoryByteCount());
            assertTrue(allocated <= BUDGET);
        }

        repository.purge();
        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @Test
    void testSegmentedContentBeyondIntegerLength() throws Exception {
        final int segmentSize = 64 * 1024;
        final int segmentCount = (int) ((1L + Integer.MAX_VALUE) / segmentSize);
        final MemoryContents contents = new MemoryContents();
        contents.allocate(2L * segmentSize);
        final List<byte[]> buffers = getBuffers(contents);
        assertEquals(2, buffers.size());
        final byte[] segment = buffers.getFirst();
        assertEquals(segmentSize, segment.length);
        assertEquals(segmentSize, buffers.getLast().length);
        Arrays.fill(segment, (byte) 1);
        buffers.addAll(Collections.nCopies(segmentCount - 2, segment));
        setField(contents, "capacity", 1L + Integer.MAX_VALUE);
        setField(contents, "size", (long) Integer.MAX_VALUE);
        setField(contents, "writeBufferIndex", segmentCount - 1);
        setField(contents, "writeBufferOffset", segmentSize - 1);

        contents.write(2);
        contents.allocate(32);
        contents.write(new byte[] {3, 4}, 0, 2);
        assertEquals(3L + Integer.MAX_VALUE, contents.size());
        assertEquals(33L + Integer.MAX_VALUE, contents.capacity());

        try (final InputStream input = contents.toInputStream()) {
            input.skipNBytes(Integer.MAX_VALUE);
            assertArrayEquals(new byte[] {2, 3, 4}, input.readAllBytes());
        }

        try (final ByteCountingOutputStream exported = new ByteCountingOutputStream(OutputStream.nullOutputStream())) {
            contents.writeTo(exported);
            assertEquals(contents.size(), exported.getBytesWritten());
        }
    }

    @Test
    void testMultipleWritesCrossingBudgetSpillMidStream() throws IOException {
        final byte[] content = bytes(120);
        final ContentClaim claim = repository.create(false);
        try (final OutputStream out = repository.write(claim)) {
            out.write(content, 0, 32);
            out.write(content[32]);
            assertEquals(96L, repository.getInMemoryByteCount());
            out.write(content, 33, 27);
            out.write(content, 60, content.length - 60);
        }

        assertTrue(repository.isSpilled(claim));
        assertEquals(0L, repository.getInMemoryByteCount());
        assertEquals(120L, repository.size(claim));

        assertArrayEquals(content, readAll(claim));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInMemoryCleanupReleasesCapacityOnce(final boolean purge) throws Exception {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(1));
        assertEquals(32L, repository.getInMemoryByteCount());

        assertEquals(0, repository.decrementClaimantCount(claim));
        if (purge) {
            repository.purge();
        } else {
            assertTrue(repository.remove(claim));
        }

        assertEquals(0L, repository.getInMemoryByteCount());
        assertClaimNotTracked(claim);
        repository.remove(claim);
        repository.purge();
        assertEquals(0L, repository.getInMemoryByteCount());
        verify(flowFileRepository, never()).updateRepository(anyList());
    }

    @Test
    void testPurgeCleansOnlyUnreferencedClaims() throws Exception {
        final byte[] inMemoryContent = bytes(1);
        final ContentClaim inMemoryClaim = repository.create(false);
        assertEquals(1, repository.getClaimantCount(inMemoryClaim));
        assertEquals(2, repository.incrementClaimaintCount(inMemoryClaim));
        write(inMemoryClaim, inMemoryContent);
        assertEquals(2, repository.getClaimantCount(inMemoryClaim));
        assertFalse(inMemoryClaim.getResourceClaim().isWritable());
        assertEquals(1, repository.decrementClaimantCount(inMemoryClaim));

        final byte[] spilledContent = bytes(150);
        final ContentClaim spilledClaim = repository.create(false);
        write(spilledClaim, spilledContent);

        repository.purge();

        verify(flowFileRepository, never()).updateRepository(anyList());
        final ContentClaim discardedMemoryClaim = repository.create(false);
        write(discardedMemoryClaim, bytes(1));
        repository.decrementClaimantCount(discardedMemoryClaim);
        final ContentClaim discardedSpilledClaim = repository.create(false);
        write(discardedSpilledClaim, bytes(160));
        repository.decrementClaimantCount(discardedSpilledClaim);

        repository.purge();

        assertEquals(32L, repository.getInMemoryByteCount());
        assertArrayEquals(inMemoryContent, readAll(inMemoryClaim));
        assertArrayEquals(spilledContent, readAll(spilledClaim));
        final ArgumentCaptor<List<RepositoryRecord>> recordsCaptor = captureRepositoryRecords();
        verify(flowFileRepository).updateRepository(recordsCaptor.capture());
        assertEquals(1, recordsCaptor.getValue().size());
        final RepositoryRecord record = recordsCaptor.getValue().getFirst();
        assertEquals(RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS, record.getType());
        assertEquals(1, record.getTransientClaims().size());
        final ContentClaim cleanupClaim = record.getTransientClaims().getFirst();
        assertArrayEquals(bytes(160), readAll(cleanupClaim));
        assertEquals(0, backingRepository.getClaimantCount(cleanupClaim));
        assertClaimNotTracked(discardedMemoryClaim);
        assertClaimNotTracked(discardedSpilledClaim);
        assertEquals(0, repository.decrementClaimantCount(inMemoryClaim));
        assertClaimNotTracked(inMemoryClaim);
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

        backingRepository.initialize(new StandardContentRepositoryContext(blockingResourceClaimManager, EventReporter.NO_OP));
        final SpillableContentRepository concurrentRepository = new SpillableContentRepository(
            backingRepository, flowFileRepository, BUDGET);
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
    void testPurgePreservesClaimWithOpenWriter() throws Exception {
        final byte[] content = bytes(50);
        final ContentClaim claim = repository.create(false);
        try (final OutputStream out = repository.write(claim)) {
            assertThrows(IllegalStateException.class, () -> repository.write(claim));
            out.write(content);
            repository.decrementClaimantCount(claim);
            repository.purge();
            assertEquals(content.length, repository.getInMemoryByteCount());
        }

        assertArrayEquals(content, readAll(claim));
        assertThrows(IllegalStateException.class, () -> repository.write(claim));
        assertClaimNotTracked(claim);
        assertFalse(claim.getResourceClaim().isWritable());
        repository.purge();
        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @Test
    void testFailedImportRemovesUnwrittenClaim(@TempDir final Path directory) throws Exception {
        final ContentClaim claim = repository.create(false);
        assertThrows(IOException.class, () -> repository.importFrom(directory.resolve("missing"), claim));
        assertEquals(0, repository.decrementClaimantCount(claim));
        assertTrue(repository.remove(claim));
        repository.purge();

        assertClaimNotTracked(claim);
        assertFalse(claim.getResourceClaim().isWritable());
        assertEquals(0L, repository.getInMemoryByteCount());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 150})
    void testExportHandsOffContentOnce(final int length) throws Exception {
        final byte[] content = bytes(length);
        final ContentClaim claim = repository.create(false);
        write(claim, content);

        final boolean spilled = length > BUDGET;
        assertEquals(spilled, repository.isSpilled(claim));
        assertEquals(!spilled, repository.isHeldInMemory(claim));
        assertEquals(spilled, repository.isAccessible(claim));
        assertEquals(length, repository.size(claim));
        assertEquals(length, repository.size(claim.getResourceClaim()));
        assertArrayEquals(content, readAll(claim));
        try (final InputStream input = repository.read(claim.getResourceClaim())) {
            assertArrayEquals(content, input.readAllBytes());
        }

        final long allocated = spilled ? 0L : allocatedBytes(claim);
        assertEquals(length == 1 ? 32L : 0L, allocated);
        assertEquals(allocated, repository.getInMemoryByteCount());

        final ContentClaim exported = repository.exportForExternalUse(claim);
        assertNotSame(claim, exported);
        assertSame(exported, repository.exportForExternalUse(claim));
        assertArrayEquals(content, readAll(exported));
        assertEquals(length, exported.getLength());
        assertEquals(0L, repository.getInMemoryByteCount());
        assertEquals(1, backingRepository.writeCount.get());
        assertEquals(1, backingRepository.closedStreamCount.get());
        assertEquals(1, repository.getClaimantCount(exported));

        repository.incrementClaimaintCount(exported);
        repository.commitExportForExternalUse(claim);
        repository.commitExportForExternalUse(claim);
        assertEquals(1, repository.getClaimantCount(exported));
        repository.decrementClaimantCount(claim);
        repository.purge();
        assertArrayEquals(content, readAll(exported));
        verify(flowFileRepository, never()).updateRepository(anyList());
    }

    @Test
    void testExportFailureSubmitsBackingClaimForCleanup() throws IOException {
        final ContentClaim claim = repository.create(false);
        write(claim, bytes(50));
        backingRepository.failNextWrite = true;

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
    void testSpillFailureRetainsBufferedContentAndMemoryAccounting() throws IOException {
        final byte[] content = bytes(60);
        final ContentClaim claim = repository.create(false);
        final OutputStream out = repository.write(claim);
        out.write(content);
        backingRepository.failNextWrite = true;

        assertThrows(IOException.class, () -> out.write(bytes(60)));
        out.close();

        assertEquals(60L, repository.getInMemoryByteCount());
        assertArrayEquals(content, readAll(claim));
    }

    @Test
    void testDeferredRepositoryResolvesChangedThreshold() throws Exception {
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final AtomicLong threshold = new AtomicLong(BUDGET);
        when(processGroup.resolveStatelessContentMaxHeap()).thenAnswer(invocation -> threshold.get());
        final DeferredStatelessContentRepository deferredRepository = new DeferredStatelessContentRepository(
            processGroup, backingRepository, flowFileRepository, resourceClaimManager, EventReporter.NO_OP);

        final ContentClaim inMemoryClaim = deferredRepository.create(false);
        assertEquals("in-memory", inMemoryClaim.getResourceClaim().getContainer());
        deferredRepository.decrementClaimantCount(inMemoryClaim);
        deferredRepository.purge();
        assertClaimNotTracked(inMemoryClaim);
        assertFalse(inMemoryClaim.getResourceClaim().isWritable());

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

    @ParameterizedTest
    @ValueSource(ints = {50, 150})
    void testImportAndExport(final int length, @TempDir final Path tempDirectory) throws IOException {
        final byte[] content = bytes(length);
        final ContentClaim claim = repository.create(false);
        final long imported = repository.importFrom(new ByteArrayInputStream(content), claim);
        assertEquals(length, imported);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(length, repository.exportTo(claim, out));
        assertArrayEquals(content, out.toByteArray());

        final Path destination = tempDirectory.resolve("content");
        Files.write(destination, bytes(length * 2));
        assertEquals(length, repository.exportTo(claim, destination, false));
        assertArrayEquals(content, Files.readAllBytes(destination));
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

    private void assertClaimNotTracked(final ContentClaim claim) throws ReflectiveOperationException {
        final ResourceClaim resourceClaim = claim.getResourceClaim();
        final Field countsField = StandardResourceClaimManager.class.getDeclaredField("claimantCounts");
        countsField.setAccessible(true);
        final Map<?, ?> claimantCounts = (Map<?, ?>) countsField.get(resourceClaimManager);
        assertFalse(claimantCounts.containsKey(resourceClaim));
        assertNull(resourceClaimManager.getResourceClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId()));
        assertEquals(0, resourceClaimManager.getClaimantCount(resourceClaim));
    }

    private long allocatedBytes(final ContentClaim claim) throws ReflectiveOperationException {
        final Object resourceClaim = claim.getResourceClaim();
        final Field contentsField = resourceClaim.getClass().getDeclaredField("contents");
        contentsField.setAccessible(true);
        final MemoryContents contents = (MemoryContents) contentsField.get(resourceClaim);
        long allocated = 0L;
        for (final byte[] buffer : getBuffers(contents)) {
            allocated += buffer.length;
        }

        return allocated;
    }

    @SuppressWarnings("unchecked")
    private List<byte[]> getBuffers(final MemoryContents contents) throws ReflectiveOperationException {
        final Field buffersField = MemoryContents.class.getDeclaredField("buffers");
        buffersField.setAccessible(true);
        return (List<byte[]>) buffersField.get(contents);
    }

    private void setField(final MemoryContents contents, final String name, final Object value) throws ReflectiveOperationException {
        final Field field = MemoryContents.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(contents, value);
    }

    private static byte[] bytes(final int length) {
        final byte[] content = new byte[length];
        for (int i = 0; i < length; i++) {
            content[i] = (byte) ('A' + (i % 26));
        }

        return content;
    }

    private static final class TrackingContentRepository extends ByteArrayContentRepository {
        private final AtomicInteger writeCount = new AtomicInteger(0);
        private final AtomicInteger closedStreamCount = new AtomicInteger(0);
        private boolean failNextWrite;

        @Override
        public OutputStream write(final ContentClaim claim) {
            writeCount.incrementAndGet();
            final boolean failWrite = failNextWrite;
            failNextWrite = false;
            return new FilterOutputStream(super.write(claim)) {
                @Override
                public void write(final int value) throws IOException {
                    if (failWrite) {
                        throw new IOException("Write failed");
                    }

                    out.write(value);
                }

                @Override
                public void write(final byte[] bytes, final int offset, final int length) throws IOException {
                    if (failWrite) {
                        throw new IOException("Write failed");
                    }

                    out.write(bytes, offset, length);
                }

                @Override
                public void close() throws IOException {
                    super.close();
                    closedStreamCount.incrementAndGet();
                }
            };
        }

        @Override
        public boolean isAccessible(final ContentClaim contentClaim) {
            return contentClaim != null && getBytes(contentClaim) != null;
        }
    }
}
