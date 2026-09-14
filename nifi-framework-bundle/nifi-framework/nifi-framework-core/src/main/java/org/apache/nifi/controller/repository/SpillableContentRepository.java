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

import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link ContentRepository} used by an embedded Stateless Process Group that buffers FlowFile content in memory up to a configured total size and spills to a
 * backing (on-disk) Content Repository once that size is exceeded. Content for a single {@link ContentClaim} is stored either entirely in memory or entirely in
 * the backing repository: while a claim is being written, each write checks the running total of buffered bytes and, if the write would exceed the configured
 * size, the bytes buffered so far are flushed to the backing repository and the remainder of the claim is written there.
 *
 * <p>
 * Claimant counts for in-memory claims are tracked in the {@link ResourceClaimManager} provided at initialization, the same manager used by the backing
 * repository. Each claim that spills holds a single claimant count on its backing claim that this repository owns. Exporting prepares the backing claim while
 * retaining that ownership until {@link #commitExportForExternalUse(ContentClaim)} confirms that the NiFi FlowFile Repository references it. If export does not
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
        final SpillableContentClaim contentClaim = new SpillableContentClaim(lossTolerant);
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
     * Ensures that the content for the given claim is accessible outside of this Stateless Process Group by making it available in the backing (on-disk) Content
     * Repository, and returns a Content Claim that references it there. For a claim whose content was buffered in memory, the content is written to the backing
     * repository and a new backing claim is returned. This repository retains ownership of the backing claim until
     * {@link #commitExportForExternalUse(ContentClaim)} is called. For any claim that does not belong to this repository, the claim is returned unchanged.
     *
     * @param claim the claim to make externally accessible
     * @return a Content Claim whose content is stored in the backing Content Repository
     * @throws IOException if the content cannot be written to the backing repository
     */
    public ContentClaim exportForExternalUse(final ContentClaim claim) throws IOException {
        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
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
     * @param claim the original claim passed to {@link #exportForExternalUse(ContentClaim)}
     */
    public void commitExportForExternalUse(final ContentClaim claim) {
        if (!(claim instanceof final SpillableContentClaim spillableClaim)) {
            return;
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
        private final byte[] singleByte = new byte[1];
        private UnsynchronizedByteArrayOutputStream buffer = UnsynchronizedByteArrayOutputStream.builder().get();
        private long reserved = 0L;
        private OutputStream spillStream;
        private boolean closed = false;

        private SpillableOutputStream(final SpillableContentClaim contentClaim) {
            this.contentClaim = contentClaim;
            this.lossTolerant = contentClaim.isLossTolerant();
        }

        @Override
        public void write(final int b) throws IOException {
            singleByte[0] = (byte) b;
            write(singleByte, 0, 1);
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

            if (reserveMemory(len)) {
                buffer.write(b, off, len);
                reserved += len;
                return;
            }

            spillOver();
            spillStream.write(b, off, len);
        }

        private boolean reserveMemory(final int bytes) {
            while (true) {
                final long currentMemoryUsed = memoryUsed.get();
                if (currentMemoryUsed > memoryThresholdBytes - bytes) {
                    return false;
                }

                if (memoryUsed.compareAndSet(currentMemoryUsed, currentMemoryUsed + bytes)) {
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
            buffer = null;
            memoryUsed.addAndGet(-reserved);
            logger.debug("Spilled Content Claim {} to the Content Repository after buffering {} bytes in memory; in-memory budget is {} bytes", contentClaim.getResourceClaim().getId(), reserved,
                memoryThresholdBytes);
            reserved = 0L;
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

    private static final class SpillableContentClaim implements ContentClaim {
        private final SpillableResourceClaim resourceClaim;

        private SpillableContentClaim(final boolean lossTolerant) {
            this.resourceClaim = new SpillableResourceClaim(lossTolerant);
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

        private void storeInMemory(final UnsynchronizedByteArrayOutputStream contents) {
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

    private static final class SpillableResourceClaim implements ResourceClaim {
        private static final AtomicLong idCounter = new AtomicLong(0L);

        private final String id = String.valueOf(idCounter.getAndIncrement());
        private final boolean lossTolerant;
        private volatile UnsynchronizedByteArrayOutputStream contents;
        private volatile ContentClaim backingClaim;
        private volatile BackingClaimOwnership backingClaimOwnership = BackingClaimOwnership.NONE;
        private volatile boolean writeStarted;
        private volatile boolean writeFinished;
        private volatile boolean discarded = false;

        private SpillableResourceClaim(final boolean lossTolerant) {
            this.lossTolerant = lossTolerant;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String getContainer() {
            return "in-memory";
        }

        @Override
        public String getSection() {
            return "in-memory";
        }

        @Override
        public boolean isLossTolerant() {
            return lossTolerant;
        }

        @Override
        public boolean isWritable() {
            return !discarded && (!writeStarted || !writeFinished);
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

            final UnsynchronizedByteArrayOutputStream currentContents = contents;
            return currentContents == null ? 0L : currentContents.size();
        }

        private synchronized boolean beginWrite() {
            if (writeStarted || discarded || contents != null || backingClaim != null) {
                return false;
            }

            writeStarted = true;
            return true;
        }

        private synchronized void finishWrite() {
            writeFinished = true;
        }

        private boolean isWriteInProgress() {
            return writeStarted && !writeFinished;
        }

        private synchronized void storeInMemory(final UnsynchronizedByteArrayOutputStream contents) {
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

            final UnsynchronizedByteArrayOutputStream currentContents = contents;
            contents = null;
            discarded = true;
            return currentContents == null ? 0L : currentContents.size();
        }

        private boolean hasInMemoryContents() {
            return contents != null;
        }

        private void writeInMemoryTo(final OutputStream out) throws IOException {
            final UnsynchronizedByteArrayOutputStream currentContents = contents;
            if (currentContents != null) {
                currentContents.writeTo(out);
            }
        }

        private InputStream readInMemory() {
            final UnsynchronizedByteArrayOutputStream currentContents = contents;
            if (currentContents == null) {
                return InputStream.nullInputStream();
            }

            return currentContents.toInputStream();
        }

        private synchronized ReleasedContent releaseForCleanup() {
            final UnsynchronizedByteArrayOutputStream currentContents = contents;
            contents = null;
            discarded = true;

            final ContentClaim cleanupClaim;
            if (backingClaimOwnership == BackingClaimOwnership.REPOSITORY) {
                cleanupClaim = backingClaim;
                backingClaimOwnership = BackingClaimOwnership.NONE;
            } else {
                cleanupClaim = null;
            }

            return new ReleasedContent(currentContents == null ? 0L : currentContents.size(), cleanupClaim);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final SpillableResourceClaim that = (SpillableResourceClaim) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    private enum BackingClaimOwnership {
        NONE,
        REPOSITORY,
        EXTERNAL
    }
}
