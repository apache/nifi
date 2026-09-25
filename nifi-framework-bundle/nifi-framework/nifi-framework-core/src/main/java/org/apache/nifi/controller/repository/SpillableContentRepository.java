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
import org.apache.nifi.controller.repository.claim.StandardResourceClaim;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link ContentRepository} used by an embedded Stateless Process Group that buffers FlowFile content in memory up to a configured total size and spills to a
 * backing Content Repository once that size is exceeded. Content for a single {@link ContentClaim} is stored either entirely in memory or entirely in
 * the backing repository: while a claim is being written, each allocation reserves its buffer capacity against the configured size. If capacity is unavailable,
 * the bytes buffered so far are flushed to the backing repository and the remainder of the claim is written there. The limit covers content byte arrays,
 * including unused capacity, but not object metadata or buffers owned by processors and their streams.
 *
 * <p>
 * Claimant counts for in-memory claims are tracked in the {@link ResourceClaimManager} provided at initialization, the same manager used by the backing
 * repository. Each claim that spills holds a single claimant count on its backing claim that this repository owns. Preparing a backing claim retains that ownership
 * until {@link #completeBackingClaimTransfer(ContentClaim)} confirms that the NiFi FlowFile Repository references it. If the transfer does not
 * complete, purge releases the backing claim for cleanup. In-memory claims that report {@link ResourceClaim#isInUse()} as {@code true} are never handed to the NiFi
 * FlowFile Repository for destruction; they are reclaimed by garbage collection once no FlowFile references them, and their memory accounting is released when
 * the claim is removed or the repository is purged.
 */
public class SpillableContentRepository implements ContentRepository {
    private static final Logger logger = LoggerFactory.getLogger(SpillableContentRepository.class);

    private final ContentRepository backingRepository;
    private final FlowFileRepository nifiFlowFileRepository;
    private final long memoryThresholdBytes;
    private final AtomicLong memoryUsed = new AtomicLong(0L);
    private final Set<SpillableContentClaim> activeClaims = ConcurrentHashMap.newKeySet();
    private final Set<ContentClaim> backingClaimsPendingCleanup = ConcurrentHashMap.newKeySet();

    private volatile ResourceClaimManager resourceClaimManager;

    public SpillableContentRepository(final ContentRepository backingRepository, final FlowFileRepository nifiFlowFileRepository, final long memoryThresholdBytes) {
        this.backingRepository = backingRepository;
        this.nifiFlowFileRepository = nifiFlowFileRepository;
        this.memoryThresholdBytes = memoryThresholdBytes;
    }

    @Override
    public void initialize(final ContentRepositoryContext context) {
        this.resourceClaimManager = context.getResourceClaimManager();
    }

    @Override
    public void shutdown() {
        purge();
    }

    @Override
    public Set<String> getContainerNames() {
        return backingRepository.getContainerNames();
    }

    @Override
    public long getContainerCapacity(final String containerName) throws IOException {
        return backingRepository.getContainerCapacity(containerName);
    }

    @Override
    public long getContainerUsableSpace(final String containerName) throws IOException {
        return backingRepository.getContainerUsableSpace(containerName);
    }

    @Override
    public String getContainerFileStoreName(final String containerName) {
        return backingRepository.getContainerFileStoreName(containerName);
    }

    @Override
    public ContentClaim create(final boolean lossTolerant) {
        final SpillableContentClaim contentClaim = new SpillableContentClaim(resourceClaimManager, lossTolerant);
        resourceClaimManager.incrementClaimantCount(contentClaim.getResourceClaim());
        activeClaims.add(contentClaim);
        return contentClaim;
    }

    @Override
    public int incrementClaimaintCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        if (claim instanceof SpillableContentClaim) {
            return resourceClaimManager.incrementClaimantCount(claim.getResourceClaim());
        }

        return backingRepository.incrementClaimaintCount(claim);
    }

    @Override
    public int getClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        if (claim instanceof SpillableContentClaim) {
            return resourceClaimManager.getClaimantCount(claim.getResourceClaim());
        }

        return backingRepository.getClaimantCount(claim);
    }

    @Override
    public int decrementClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        if (claim instanceof SpillableContentClaim) {
            return resourceClaimManager.decrementClaimantCount(claim.getResourceClaim());
        }

        return backingRepository.decrementClaimantCount(claim);
    }

    @Override
    public boolean remove(final ContentClaim claim) {
        if (claim == null) {
            return true;
        }

        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            return backingRepository.remove(claim);
        }

        releaseClaimForCleanup(spillableClaim);
        return true;
    }

    @Override
    public ContentClaim clone(final ContentClaim original, final boolean lossTolerant) throws IOException {
        final ContentClaim clone = create(lossTolerant);
        try (final InputStream in = read(original);
             final OutputStream out = write(clone)) {
            in.transferTo(out);
        } catch (final IOException | RuntimeException e) {
            decrementClaimantCount(clone);
            remove(clone);
            throw e;
        }

        return clone;
    }

    @Override
    public long importFrom(final Path content, final ContentClaim claim) throws IOException {
        try (final InputStream in = Files.newInputStream(content, StandardOpenOption.READ)) {
            return importFrom(in, claim);
        }
    }

    @Override
    public long importFrom(final InputStream content, final ContentClaim claim) throws IOException {
        try (final OutputStream out = write(claim)) {
            return content.transferTo(out);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append) throws IOException {
        final OpenOption[] openOptions = append ? new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.APPEND} :
            new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};

        try (final OutputStream out = Files.newOutputStream(destination, openOptions)) {
            return exportTo(claim, out);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append, final long offset, final long length) throws IOException {
        final OpenOption[] openOptions = append ? new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.APPEND} :
            new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};

        try (final OutputStream out = Files.newOutputStream(destination, openOptions)) {
            return exportTo(claim, out, offset, length);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final OutputStream destination) throws IOException {
        try (final InputStream in = read(claim)) {
            return in.transferTo(destination);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final OutputStream destination, final long offset, final long length) throws IOException {
        try (final InputStream in = read(claim)) {
            StreamUtils.skip(in, offset);
            StreamUtils.copy(in, destination, length);
        }

        return length;
    }

    @Override
    public long size(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return 0;
        }

        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            return backingRepository.size(claim);
        }

        final ContentClaim backingClaim = spillableClaim.getBackingClaim();
        if (backingClaim != null) {
            return backingRepository.size(backingClaim);
        }

        return spillableClaim.getLength();
    }

    @Override
    public long size(final ResourceClaim claim) throws IOException {
        if (claim instanceof final SpillableResourceClaim spillableResourceClaim) {
            final ContentClaim backingClaim = spillableResourceClaim.getBackingClaim();
            return backingClaim == null ? spillableResourceClaim.getLength() : backingRepository.size(backingClaim);
        }

        return backingRepository.size(claim);
    }

    @Override
    public InputStream read(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return InputStream.nullInputStream();
        }

        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            return backingRepository.read(claim);
        }

        final ContentClaim backingClaim = spillableClaim.getBackingClaim();
        if (backingClaim != null) {
            return backingRepository.read(backingClaim);
        }

        return spillableClaim.readInMemory();
    }

    @Override
    public InputStream read(final ResourceClaim claim) throws IOException {
        if (claim instanceof final SpillableResourceClaim spillableResourceClaim) {
            final ContentClaim backingClaim = spillableResourceClaim.getBackingClaim();
            return backingClaim == null ? spillableResourceClaim.readInMemory() : backingRepository.read(backingClaim);
        }

        return backingRepository.read(claim);
    }

    @Override
    public OutputStream write(final ContentClaim claim) throws IOException {
        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            return backingRepository.write(claim);
        }

        if (!spillableClaim.beginWrite()) {
            throw new IllegalStateException("Cannot write to Content Claim because it has already been written to or has an active writer");
        }

        return new SpillableOutputStream(spillableClaim);
    }

    @Override
    public void purge() {
        for (final SpillableContentClaim contentClaim : activeClaims) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            synchronized (resourceClaim) {
                if (resourceClaimManager.getClaimantCount(resourceClaim) == 0 && !contentClaim.isWriteInProgress() && activeClaims.remove(contentClaim)) {
                    releaseClaimContentsForCleanup(contentClaim);
                }
            }
        }

        submitBackingClaimsForCleanup();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public boolean isAccessible(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return false;
        }

        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            return backingRepository.isAccessible(claim);
        }

        final ContentClaim backingClaim = spillableClaim.getBackingClaim();
        if (backingClaim != null) {
            return backingRepository.isAccessible(backingClaim);
        }

        return false;
    }

    /**
     * Returns a claim from the backing Content Repository. For a claim whose content was buffered in memory, the content is written to the backing repository
     * and a new backing claim is returned. This repository retains ownership of the backing claim until
     * {@link #completeBackingClaimTransfer(ContentClaim)} is called. For any claim that does not belong to this repository, the claim is returned unchanged.
     *
     * @param claim the claim to prepare
     * @return a Content Claim whose content is stored in the backing Content Repository
     * @throws IOException if the content cannot be written to the backing repository
     */
    ContentClaim prepareBackingClaim(final ContentClaim claim) throws IOException {
        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            // Input FlowFiles can already reference claims from the backing Content Repository.
            return claim;
        }

        final SpillableResourceClaim resourceClaim = spillableClaim.getResourceClaim();
        synchronized (resourceClaim) {
            final ContentClaim existingBackingClaim = spillableClaim.getBackingClaim();
            if (existingBackingClaim != null) {
                return existingBackingClaim;
            }

            if (spillableClaim.isWriteInProgress()) {
                throw new IllegalStateException("Cannot export Content Claim while it is being written");
            }

            final ContentClaim backingClaim = backingRepository.create(spillableClaim.isLossTolerant());
            try (final OutputStream out = backingRepository.write(backingClaim)) {
                spillableClaim.writeInMemoryTo(out);
            } catch (final IOException | RuntimeException e) {
                releaseBackingClaimForCleanup(backingClaim);
                throw e;
            }

            final long freed = spillableClaim.markExportPrepared(backingClaim);
            if (freed > 0) {
                memoryUsed.addAndGet(-freed);
            }

            return backingClaim;
        }
    }

    /**
     * Hands ownership of a prepared backing claim to the NiFi FlowFile Repository after its records have been updated successfully.
     *
     * @param claim the original claim passed to {@link #prepareBackingClaim(ContentClaim)}
     */
    void completeBackingClaimTransfer(final ContentClaim claim) {
        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            throw new IllegalArgumentException("Content Claim was not created by this repository");
        }

        final SpillableResourceClaim resourceClaim = spillableClaim.getResourceClaim();
        synchronized (resourceClaim) {
            final ContentClaim backingClaim = spillableClaim.getBackingClaim();
            if (backingClaim == null) {
                return;
            }

            if (spillableClaim.transferBackingClaimOwnership()) {
                backingRepository.decrementClaimantCount(backingClaim);
            }

            activeClaims.remove(spillableClaim);
        }
    }

    private void releaseClaimForCleanup(final SpillableContentClaim contentClaim) {
        final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
        synchronized (resourceClaim) {
            if (activeClaims.remove(contentClaim)) {
                releaseClaimContentsForCleanup(contentClaim);
            }
        }
    }

    private void releaseClaimContentsForCleanup(final SpillableContentClaim contentClaim) {
        final ReleasedContent releasedContent = contentClaim.releaseForCleanup();
        if (releasedContent.memoryBytes() > 0) {
            memoryUsed.addAndGet(-releasedContent.memoryBytes());
        }

        if (releasedContent.backingClaim() != null) {
            releaseBackingClaimForCleanup(releasedContent.backingClaim());
        }
    }

    private void releaseBackingClaimForCleanup(final ContentClaim backingClaim) {
        backingRepository.decrementClaimantCount(backingClaim);
        backingClaimsPendingCleanup.add(backingClaim);
    }

    private synchronized void submitBackingClaimsForCleanup() {
        if (backingClaimsPendingCleanup.isEmpty()) {
            return;
        }

        final Set<ContentClaim> claimsToDestroy = new HashSet<>(backingClaimsPendingCleanup);
        try {
            nifiFlowFileRepository.updateRepository(List.of(new StandardRepositoryRecord(claimsToDestroy)));
            backingClaimsPendingCleanup.removeAll(claimsToDestroy);
        } catch (final IOException e) {
            logger.warn("Failed to submit spilled Content Claims for cleanup", e);
        }
    }

    long getInMemoryByteCount() {
        return memoryUsed.get();
    }

    boolean isHeldInMemory(final ContentClaim claim) {
        return claim instanceof final SpillableContentClaim spillableClaim && spillableClaim.getBackingClaim() == null && spillableClaim.hasInMemoryContents();
    }

    boolean isSpilled(final ContentClaim claim) {
        return claim instanceof final SpillableContentClaim spillableClaim && spillableClaim.getBackingClaim() != null;
    }

    private record ReleasedContent(long memoryBytes, ContentClaim backingClaim) {
    }

    private final class SpillableOutputStream extends OutputStream {
        private final SpillableContentClaim contentClaim;
        private final boolean lossTolerant;
        private MemoryContents buffer = new MemoryContents();
        private OutputStream spillStream;
        private boolean closed = false;

        private SpillableOutputStream(final SpillableContentClaim contentClaim) {
            this.contentClaim = contentClaim;
            this.lossTolerant = contentClaim.isLossTolerant();
        }

        @Override
        public void write(final int value) throws IOException {
            if (closed) {
                throw new IOException("Cannot write to closed stream");
            }

            if (spillStream == null && !reserveCapacity(1)) {
                spillOver();
            }

            if (spillStream == null) {
                buffer.write(value);
            } else {
                spillStream.write(value);
            }
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            Objects.requireNonNull(b);
            Objects.checkFromIndexSize(off, len, b.length);
            if (closed) {
                throw new IOException("Cannot write to closed stream");
            }

            if (len == 0) {
                return;
            }

            if (spillStream != null) {
                spillStream.write(b, off, len);
                return;
            }

            if (reserveCapacity(len)) {
                buffer.write(b, off, len);
                return;
            }

            spillOver();
            spillStream.write(b, off, len);
        }

        private boolean reserveCapacity(final int bytes) {
            final long required = bytes - (buffer.capacity() - buffer.size());
            if (required <= 0) {
                return true;
            }

            final long preferred = Math.max(buffer.nextBufferSize(), required);
            while (true) {
                final long currentMemoryUsed = memoryUsed.get();
                final long available = memoryThresholdBytes - currentMemoryUsed;
                if (available < required) {
                    return false;
                }

                final long reserved = Math.min(preferred, available);
                if (memoryUsed.compareAndSet(currentMemoryUsed, currentMemoryUsed + reserved)) {
                    try {
                        buffer.allocate(reserved);
                    } catch (final RuntimeException | Error e) {
                        memoryUsed.addAndGet(-reserved);
                        throw e;
                    }

                    return true;
                }
            }
        }

        private void spillOver() throws IOException {
            final ContentClaim createdSpillClaim = backingRepository.create(lossTolerant);
            OutputStream createdSpillStream = null;
            try {
                createdSpillStream = backingRepository.write(createdSpillClaim);
                buffer.writeTo(createdSpillStream);
                contentClaim.markSpilled(createdSpillClaim);
            } catch (final IOException | RuntimeException e) {
                if (createdSpillStream != null) {
                    try {
                        createdSpillStream.close();
                    } catch (final Exception closeException) {
                        e.addSuppressed(closeException);
                    }
                }

                releaseBackingClaimForCleanup(createdSpillClaim);
                throw e;
            }

            spillStream = createdSpillStream;
            final long reserved = buffer.capacity();
            buffer = null;
            memoryUsed.addAndGet(-reserved);
            logger.debug("Spilled Content Claim {} to the Content Repository after buffering {} bytes in memory; in-memory budget is {} bytes", contentClaim.getResourceClaim().getId(), reserved,
                memoryThresholdBytes);
        }

        @Override
        public void flush() throws IOException {
            if (spillStream != null) {
                spillStream.flush();
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            closed = true;

            try {
                if (spillStream != null) {
                    spillStream.close();
                    return;
                }

                contentClaim.storeInMemory(buffer);
                buffer = null;
            } finally {
                contentClaim.finishWrite();
            }
        }
    }

    static final class MemoryContents {
        private static final int MAX_BUFFER_SIZE = 64 * 1024;

        private final List<byte[]> buffers = new ArrayList<>();
        private long capacity;
        private long size;
        private int writeBufferIndex;
        private int writeBufferOffset;

        int nextBufferSize() {
            return buffers.isEmpty() ? 32 : Math.min(MAX_BUFFER_SIZE, buffers.getLast().length * 2);
        }

        void allocate(final long bytes) {
            final int originalBufferCount = buffers.size();
            try {
                long remaining = bytes;
                while (remaining > 0) {
                    final int bufferSize = (int) Math.min(MAX_BUFFER_SIZE, remaining);
                    buffers.add(new byte[bufferSize]);
                    remaining -= bufferSize;
                }
            } catch (final RuntimeException | Error e) {
                buffers.subList(originalBufferCount, buffers.size()).clear();
                throw e;
            }

            capacity += bytes;
        }

        void write(final int value) {
            final byte[] currentBuffer = writableBuffer();
            currentBuffer[writeBufferOffset++] = (byte) value;
            size++;
        }

        private byte[] writableBuffer() {
            if (writeBufferOffset == buffers.get(writeBufferIndex).length) {
                writeBufferIndex++;
                writeBufferOffset = 0;
            }

            return buffers.get(writeBufferIndex);
        }

        void write(final byte[] source, final int offset, final int length) {
            int remaining = length;
            while (remaining > 0) {
                final byte[] currentBuffer = writableBuffer();
                final int copied = Math.min(remaining, currentBuffer.length - writeBufferOffset);
                System.arraycopy(source, offset + length - remaining, currentBuffer, writeBufferOffset, copied);
                writeBufferOffset += copied;
                size += copied;
                remaining -= copied;
            }
        }

        long size() {
            return size;
        }

        long capacity() {
            return capacity;
        }

        void writeTo(final OutputStream destination) throws IOException {
            long remaining = size;
            for (final byte[] currentBuffer : buffers) {
                final int length = (int) Math.min(remaining, currentBuffer.length);
                destination.write(currentBuffer, 0, length);
                remaining -= length;
                if (remaining == 0) {
                    break;
                }
            }
        }

        InputStream toInputStream() {
            if (size == 0) {
                return InputStream.nullInputStream();
            }

            if (buffers.size() == 1) {
                return new ByteArrayInputStream(buffers.getFirst(), 0, (int) size);
            }

            final Iterator<byte[]> iterator = buffers.iterator();
            final Enumeration<InputStream> streams = new Enumeration<>() {
                private long remaining = size;

                @Override
                public boolean hasMoreElements() {
                    return remaining > 0 && iterator.hasNext();
                }

                @Override
                public InputStream nextElement() {
                    final byte[] currentBuffer = iterator.next();
                    final int length = (int) Math.min(remaining, currentBuffer.length);
                    remaining -= length;
                    return new ByteArrayInputStream(currentBuffer, 0, length);
                }
            };
            return new SequenceInputStream(streams);
        }
    }

    private static final class SpillableContentClaim implements ContentClaim {
        private final SpillableResourceClaim resourceClaim;

        private SpillableContentClaim(final ResourceClaimManager claimManager, final boolean lossTolerant) {
            this.resourceClaim = new SpillableResourceClaim(claimManager, lossTolerant);
        }

        @Override
        public SpillableResourceClaim getResourceClaim() {
            return resourceClaim;
        }

        @Override
        public long getOffset() {
            return 0L;
        }

        @Override
        public long getLength() {
            return resourceClaim.getLength();
        }

        @Override
        public boolean isTruncationCandidate() {
            return false;
        }

        private boolean isLossTolerant() {
            return resourceClaim.isLossTolerant();
        }

        private boolean beginWrite() {
            return resourceClaim.beginWrite();
        }

        private void finishWrite() {
            resourceClaim.finishWrite();
        }

        private boolean isWriteInProgress() {
            return resourceClaim.isWriteInProgress();
        }

        private void storeInMemory(final MemoryContents contents) {
            resourceClaim.storeInMemory(contents);
        }

        private void markSpilled(final ContentClaim backingClaim) {
            resourceClaim.markSpilled(backingClaim);
        }

        private ContentClaim getBackingClaim() {
            return resourceClaim.getBackingClaim();
        }

        private boolean transferBackingClaimOwnership() {
            return resourceClaim.transferBackingClaimOwnership();
        }

        private long markExportPrepared(final ContentClaim backingClaim) {
            return resourceClaim.markExportPrepared(backingClaim);
        }

        private boolean hasInMemoryContents() {
            return resourceClaim.hasInMemoryContents();
        }

        private void writeInMemoryTo(final OutputStream out) throws IOException {
            resourceClaim.writeInMemoryTo(out);
        }

        private InputStream readInMemory() {
            return resourceClaim.readInMemory();
        }

        private ReleasedContent releaseForCleanup() {
            return resourceClaim.releaseForCleanup();
        }

        @Override
        public int compareTo(final ContentClaim o) {
            return resourceClaim.compareTo(o.getResourceClaim());
        }

        @Override
        public int hashCode() {
            return resourceClaim.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            return this == obj;
        }
    }

    private static final class SpillableResourceClaim extends StandardResourceClaim {
        private static final AtomicLong idCounter = new AtomicLong(0L);

        private final ResourceClaimManager claimManager;
        private volatile MemoryContents contents;
        private volatile ContentClaim backingClaim;
        private volatile BackingClaimOwnership backingClaimOwnership = BackingClaimOwnership.NONE;
        private volatile boolean writeStarted;
        private volatile boolean writeFinished;
        private volatile boolean discarded = false;

        private SpillableResourceClaim(final ResourceClaimManager claimManager, final boolean lossTolerant) {
            super(claimManager, "in-memory", "in-memory", String.valueOf(idCounter.getAndIncrement()), lossTolerant);
            this.claimManager = claimManager;
        }

        @Override
        public boolean isInUse() {
            return true;
        }

        private long getLength() {
            final ContentClaim currentBackingClaim = backingClaim;
            if (currentBackingClaim != null) {
                return currentBackingClaim.getLength();
            }

            final MemoryContents currentContents = contents;
            return currentContents == null ? 0L : currentContents.size();
        }

        private synchronized boolean beginWrite() {
            if (writeStarted || discarded || !isWritable() || contents != null || backingClaim != null) {
                return false;
            }

            writeStarted = true;
            return true;
        }

        private synchronized void finishWrite() {
            writeFinished = true;
            claimManager.freeze(this);
        }

        private boolean isWriteInProgress() {
            return writeStarted && !writeFinished;
        }

        private synchronized void storeInMemory(final MemoryContents contents) {
            this.contents = contents;
        }

        private synchronized void markSpilled(final ContentClaim backingClaim) {
            this.backingClaim = backingClaim;
            backingClaimOwnership = BackingClaimOwnership.REPOSITORY;
        }

        private ContentClaim getBackingClaim() {
            return backingClaim;
        }

        private synchronized boolean transferBackingClaimOwnership() {
            if (backingClaimOwnership != BackingClaimOwnership.REPOSITORY) {
                return false;
            }

            backingClaimOwnership = BackingClaimOwnership.EXTERNAL;
            return true;
        }

        private synchronized long markExportPrepared(final ContentClaim backingClaim) {
            this.backingClaim = backingClaim;
            backingClaimOwnership = BackingClaimOwnership.REPOSITORY;

            final MemoryContents currentContents = contents;
            contents = null;
            discarded = true;
            claimManager.freeze(this);
            return currentContents == null ? 0L : currentContents.capacity();
        }

        private boolean hasInMemoryContents() {
            return contents != null;
        }

        private void writeInMemoryTo(final OutputStream out) throws IOException {
            final MemoryContents currentContents = contents;
            if (currentContents != null) {
                currentContents.writeTo(out);
            }
        }

        private InputStream readInMemory() {
            final MemoryContents currentContents = contents;
            if (currentContents == null) {
                return InputStream.nullInputStream();
            }

            return currentContents.toInputStream();
        }

        private synchronized ReleasedContent releaseForCleanup() {
            final MemoryContents currentContents = contents;
            contents = null;
            discarded = true;
            claimManager.freeze(this);

            final ContentClaim cleanupClaim;
            if (backingClaimOwnership == BackingClaimOwnership.REPOSITORY) {
                cleanupClaim = backingClaim;
                backingClaimOwnership = BackingClaimOwnership.NONE;
            } else {
                cleanupClaim = null;
            }

            return new ReleasedContent(currentContents == null ? 0L : currentContents.capacity(), cleanupClaim);
        }
    }

    private enum BackingClaimOwnership {
        NONE,
        REPOSITORY,
        EXTERNAL
    }
}
