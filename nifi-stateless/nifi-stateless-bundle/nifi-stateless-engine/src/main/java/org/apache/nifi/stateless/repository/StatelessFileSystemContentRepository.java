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

package org.apache.nifi.stateless.repository;

import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.ContentRepositoryContext;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaim;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.SynchronizedByteCountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class StatelessFileSystemContentRepository implements ContentRepository {
    private static final Logger logger = LoggerFactory.getLogger(StatelessFileSystemContentRepository.class);
    private static final String CONTENT_FILE_REGEX = "\\d+\\.nifi\\.bin";

    private static final String CONTAINER = "stateless";
    private static final String SECTION = "stateless";

    private final File directory;
    private final ConcurrentMap<ResourceClaim, SynchronizedByteCountingOutputStream> writableStreamMap = new ConcurrentHashMap<>();
    private final AtomicLong resourceClaimIndex = new AtomicLong(0L);
    private final BlockingQueue<ResourceClaim> writableClaimQueue = new LinkedBlockingQueue<>();
    private ResourceClaimManager resourceClaimManager;

    public StatelessFileSystemContentRepository(final File directory) {
        this.directory = directory;
    }

    @Override
    public void initialize(final ContentRepositoryContext context) throws IOException {
        this.resourceClaimManager = context.getResourceClaimManager();
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Cannot initialize Content Repository because " + directory.getAbsolutePath() + " does not exist and cannot be created");
        }

        // Check if there are any existing files and if so, purges them.
        final File[] existingFiles = directory.listFiles(file -> file.getName().matches(CONTENT_FILE_REGEX));
        if (existingFiles == null) {
            throw new IOException("Cannot initialize Content Repository because failed to list contents of directory " + directory.getAbsolutePath());
        }

        for (final File existingFile : existingFiles) {
            logger.info("Found existing file from previous run {}. Removing file.", existingFile.getName());
            final boolean deleted = existingFile.delete();

            if (!deleted) {
                logger.warn("Failed to remove existing file from previous run {}", existingFile);
            }
        }
    }

    @Override
    public void shutdown() {
        purge();
    }

    @Override
    public Set<String> getContainerNames() {
        return Collections.singleton(CONTAINER);
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
    public ContentClaim create(final boolean lossTolerant) throws IOException {
        ResourceClaim resourceClaim = writableClaimQueue.poll();
        long offset;

        if (resourceClaim == null) {
            resourceClaim = new StandardResourceClaim(resourceClaimManager, CONTAINER, SECTION, String.valueOf(resourceClaimIndex.getAndIncrement()), false);
            offset = 0L;

            final File resourceClaimFile = getFile(resourceClaim);
            final OutputStream fos = new FileOutputStream(resourceClaimFile);
            final SynchronizedByteCountingOutputStream contentOutputStream = new SynchronizedByteCountingOutputStream(fos);
            writableStreamMap.put(resourceClaim, contentOutputStream);
        } else {
            final SynchronizedByteCountingOutputStream contentOutputStream = writableStreamMap.get(resourceClaim);
            offset = contentOutputStream.getBytesWritten();
        }

        final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, offset);
        resourceClaimManager.incrementClaimantCount(contentClaim.getResourceClaim());
        return contentClaim;
    }

    @Override
    public int incrementClaimaintCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return resourceClaimManager.incrementClaimantCount(claim.getResourceClaim());
    }

    @Override
    public int getClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return resourceClaimManager.getClaimantCount(claim.getResourceClaim());
    }

    @Override
    public int decrementClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return resourceClaimManager.decrementClaimantCount(claim.getResourceClaim());
    }

    @Override
    public boolean remove(final ContentClaim claim) {
        return true;
    }

    @Override
    public ContentClaim clone(final ContentClaim original, final boolean lossTolerant) throws IOException {
        final ContentClaim clone = create(lossTolerant);
        try (final InputStream in = read(original);
             final OutputStream out = write(clone)) {
            StreamUtils.copy(in, out);
        }

        return clone;
    }

    @Override
    public long merge(final Collection<ContentClaim> claims, final ContentClaim destination, final byte[] header, final byte[] footer, final byte[] demarcator) {
        throw new UnsupportedOperationException("This never gets used");
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
            return StreamUtils.copy(content, out);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append) throws IOException {
        final OpenOption[] openOptions = append ? new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.APPEND} :
            new StandardOpenOption[] {StandardOpenOption.CREATE};

        try (final OutputStream out = Files.newOutputStream(destination, openOptions)) {
            return exportTo(claim, out);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append, final long offset, final long length) throws IOException {
        final OpenOption[] openOptions = append ? new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.APPEND} :
            new StandardOpenOption[] {StandardOpenOption.CREATE};

        try (final OutputStream out = Files.newOutputStream(destination, openOptions)) {
            return exportTo(claim, out, offset, length);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final OutputStream destination) throws IOException {
        try (final InputStream in = read(claim)) {
            return StreamUtils.copy(in, destination);
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
    public long size(final ContentClaim claim) {
        return claim.getLength();
    }

    @Override
    public long size(final ResourceClaim claim) throws IOException {
        return 0;
    }

    @Override
    public InputStream read(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        final InputStream resourceClaimIn = read(claim.getResourceClaim());
        StreamUtils.skip(resourceClaimIn, claim.getOffset());

        final InputStream limitedIn = new LimitedInputStream(resourceClaimIn, claim.getLength());
        return limitedIn;
    }

    @Override
    public InputStream read(final ResourceClaim claim) throws IOException {
        validateResourceClaim(claim);
        final File file = getFile(claim);
        return new FileInputStream(file);
    }

    private File getFile(final ResourceClaim claim) {
        return new File(directory, claim.getId() + ".nifi.bin");
    }

    private void validateResourceClaim(final ResourceClaim resourceClaim) {
        if (!CONTAINER.equals(resourceClaim.getContainer())) {
            throwInvalidResourceClaim();
        }

        if (!SECTION.equals(resourceClaim.getSection())) {
            throwInvalidResourceClaim();
        }
    }

    @Override
    public OutputStream write(final ContentClaim claim) throws IOException {
        validateContentClaimForWriting(claim);

        final SynchronizedByteCountingOutputStream out = writableStreamMap.get(claim.getResourceClaim());
        if (out == null) {
            throwInvalidContentClaim();
        }

        final StandardContentClaim scc = (StandardContentClaim) claim;
        scc.setLength(0); // Set the length to 0. Initially it will be set to -1. By setting it to 0, the repository knows that it has been written to and cannot be written to again.
        return new ContentOutputStream(out, scc);
    }

    private void validateContentClaimForWriting(final ContentClaim claim) throws IOException {
        Objects.requireNonNull(claim, "ContentClaim cannot be null");

        if (!(claim instanceof StandardContentClaim)) {
            throwInvalidContentClaim();
        }

        validateResourceClaim(claim.getResourceClaim());

        if (claim.getLength() >= 0) {
            throw new IOException("Cannot write to " + claim + " because it has already been written to.");
        }
    }

    private void throwInvalidContentClaim() {
        throw new IllegalArgumentException("The given ContentClaim does not belong to this Content Repository");
    }

    private void throwInvalidResourceClaim() {
        throw new IllegalArgumentException("The given ResourceClaim does not belong to this Content Repository");
    }

    @Override
    public void purge() {
        writableClaimQueue.clear();
        for (final OutputStream out : writableStreamMap.values()) {
            try {
                out.close();
            } catch (final IOException ioe) {
                logger.warn("Failed to close Content Repository Output Stream", ioe);
            }
        }

        for (final ResourceClaim resourceClaim : writableStreamMap.keySet()) {
            final File file = getFile(resourceClaim);
            if (!file.delete() && file.exists()) {
                logger.warn("Failed to remove file from Content Repository: " + file.getAbsolutePath());
            }
        }

        writableStreamMap.clear();
        resourceClaimManager.purge();
    }

    @Override
    public void cleanup() {
        purge();
    }

    @Override
    public boolean isAccessible(final ContentClaim contentClaim) {
        return false;
    }


    private class ContentOutputStream extends FilterOutputStream {
        private final StandardContentClaim scc;
        private final SynchronizedByteCountingOutputStream out;
        private final long initialOffset;
        private boolean closed = false;

        public ContentOutputStream(final SynchronizedByteCountingOutputStream out, final StandardContentClaim scc) {
            super(out);
            this.scc = scc;
            this.out = out;
            this.initialOffset = out.getBytesWritten();
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void write(final byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(final int b) throws IOException {
            out.write(b);
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }

            closed = true;
            super.flush();
            scc.setLength(out.getBytesWritten() - initialOffset);
            writableClaimQueue.offer(scc.getResourceClaim());
        }
    }
}
