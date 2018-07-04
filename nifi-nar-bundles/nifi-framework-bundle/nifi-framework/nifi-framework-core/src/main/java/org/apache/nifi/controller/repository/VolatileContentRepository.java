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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.io.ArrayManagedOutputStream;
import org.apache.nifi.controller.repository.io.MemoryManager;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An in-memory implementation of the {@link ContentRepository} interface. This
 * implementation stores FlowFile content in the Java heap and keeps track of
 * the number of bytes used. If the number of bytes used by FlowFile content
 * exceeds some threshold (configured via the
 * <code>nifi.volatile.content.repository.max.size</code> property in the NiFi
 * properties with a default of 100 MB), one of two situations will occur:
 * </p>
 *
 * <ul>
 * <li><b>Backup Repository:</b> If a Backup Repository has been specified (via
 * the {@link #setBackupRepository(ContentRepository)} method), the content will
 * be stored in the backup repository and all access to the FlowFile content
 * will automatically and transparently be proxied to the backup repository.
 * </li>
 * <li>
 * <b>Without Backup Repository:</b> If no Backup Repository has been specified,
 * when the threshold is exceeded, an IOException will be thrown.
 * </li>
 * </ul>
 *
 * <p>
 * When a Content Claim is created via the {@link #create(boolean)} method, if
 * the <code>lossTolerant</code> flag is set to <code>false</code>, the Backup
 * Repository will be used to create the Content Claim and any accesses to the
 * ContentClaim will be proxied to the Backup Repository. If the Backup
 * Repository has not been specified, attempting to create a non-loss-tolerant
 * ContentClaim will result in an {@link IllegalStateException} being thrown.
 * </p>
 */
public class VolatileContentRepository implements ContentRepository {

    private final Logger logger = LoggerFactory.getLogger(VolatileContentRepository.class);
    public static String CONTAINER_NAME = "in-memory";
    public static final int DEFAULT_BLOCK_SIZE_KB = 32;

    public static final String MAX_SIZE_PROPERTY = "nifi.volatile.content.repository.max.size";
    public static final String BLOCK_SIZE_PROPERTY = "nifi.volatile.content.repository.block.size";

    private final ScheduledExecutorService executor = new FlowEngine(3, "VolatileContentRepository Workers", true);
    private final ConcurrentMap<ContentClaim, ContentBlock> claimMap = new ConcurrentHashMap<>(256);
    private final AtomicLong repoSize = new AtomicLong(0L);

    private final AtomicLong idGenerator = new AtomicLong(0L);
    private final long maxBytes;
    private final MemoryManager memoryManager;

    private final ConcurrentMap<ContentClaim, ContentClaim> backupRepoClaimMap = new ConcurrentHashMap<>(256);
    private final AtomicReference<ContentRepository> backupRepositoryRef = new AtomicReference<>(null);

    private ResourceClaimManager claimManager; // effectively final

    /**
     * Default no args constructor for service loading only
     */
    public VolatileContentRepository() {
        maxBytes = 0;
        memoryManager = null;
    }

    public VolatileContentRepository(final NiFiProperties nifiProperties) {
        final String maxSize = nifiProperties.getProperty(MAX_SIZE_PROPERTY);
        final String blockSizeVal = nifiProperties.getProperty(BLOCK_SIZE_PROPERTY);

        if (maxSize == null) {
            maxBytes = (long) DataUnit.B.convert(100D, DataUnit.MB);
        } else {
            maxBytes = DataUnit.parseDataSize(maxSize, DataUnit.B).longValue();
        }

        final int blockSize;
        if (blockSizeVal == null) {
            blockSize = (int) DataUnit.B.convert(DEFAULT_BLOCK_SIZE_KB, DataUnit.KB);
        } else {
            blockSize = DataUnit.parseDataSize(blockSizeVal, DataUnit.B).intValue();
        }

        memoryManager = new MemoryManager(maxBytes, blockSize);
    }

    @Override
    public void initialize(final ResourceClaimManager claimManager) {
        this.claimManager = claimManager;

        for (int i = 0; i < 3; i++) {
            executor.scheduleWithFixedDelay(new CleanupOldClaims(), 1000, 10, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    /**
     * Specifies a Backup Repository where data should be written if this
     * Repository fills up
     *
     * @param backup repo backup
     */
    public void setBackupRepository(final ContentRepository backup) {
        final boolean updated = backupRepositoryRef.compareAndSet(null, backup);
        if (!updated) {
            throw new IllegalStateException("Cannot change BackupRepository after it has already been set");
        }
    }

    public ContentRepository getBackupRepository() {
        return backupRepositoryRef.get();
    }

    private StandardContentClaim resolveClaim(final ContentClaim claim) {
        if (!(claim instanceof StandardContentClaim)) {
            throw new IllegalArgumentException("Cannot increment ClaimantCount of " + claim + " because it does not belong to this ContentRepository");
        }

        return (StandardContentClaim) claim;
    }

    private ContentClaim getBackupClaim(final ContentClaim claim) {
        if (claim == null) {
            return null;
        }
        return backupRepoClaimMap.get(claim);
    }

    @Override
    public long getContainerCapacity(final String containerName) throws IOException {
        return maxBytes;
    }

    @Override
    public Set<String> getContainerNames() {
        return Collections.singleton(CONTAINER_NAME);
    }

    @Override
    public long getContainerUsableSpace(String containerName) throws IOException {
        return maxBytes - repoSize.get();
    }

    @Override
    public String getContainerFileStoreName(String containerName) {
        return null;
    }

    @Override
    public ContentClaim create(boolean lossTolerant) throws IOException {
        if (lossTolerant) {
            return createLossTolerant();
        } else {
            final ContentRepository backupRepo = getBackupRepository();
            if (backupRepo == null) {
                // TODO: Loss Tolerance is not yet configurable.
                // Therefore, since this is an in-memory content repository, assume that this claim is loss-tolerant if there
                // is not a backup repository
                return createLossTolerant();
            }

            final ContentClaim backupClaim = backupRepo.create(lossTolerant);
            backupRepoClaimMap.put(backupClaim, backupClaim);
            return backupClaim;
        }
    }

    private ContentClaim createLossTolerant() {
        final long id = idGenerator.getAndIncrement();
        final ResourceClaim resourceClaim = claimManager.newResourceClaim(CONTAINER_NAME, "section", String.valueOf(id), true, false);
        final ContentClaim claim = new StandardContentClaim(resourceClaim, 0L);
        final ContentBlock contentBlock = new ContentBlock(claim, repoSize);
        claimManager.incrementClaimantCount(resourceClaim, true);

        claimMap.put(claim, contentBlock);

        logger.debug("Created {} and mapped to {}", claim, contentBlock);
        return claim;
    }

    @Override
    public int incrementClaimaintCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }
        final ContentClaim backupClaim = getBackupClaim(claim);
        if (backupClaim == null) {
            return claimManager.incrementClaimantCount(resolveClaim(claim).getResourceClaim());
        } else {
            return getBackupRepository().incrementClaimaintCount(backupClaim);
        }
    }

    @Override
    public int decrementClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        final ContentClaim backupClaim = getBackupClaim(claim);
        if (backupClaim == null) {
            return claimManager.decrementClaimantCount(resolveClaim(claim).getResourceClaim());
        } else {
            return getBackupRepository().decrementClaimantCount(backupClaim);
        }
    }

    @Override
    public int getClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        final ContentClaim backupClaim = getBackupClaim(claim);
        if (backupClaim == null) {
            return claimManager.getClaimantCount(resolveClaim(claim).getResourceClaim());
        } else {
            return getBackupRepository().getClaimantCount(backupClaim);
        }
    }

    @Override
    public boolean remove(final ContentClaim claim) {
        if (claim == null) {
            return false;
        }

        final ContentClaim backupClaim = getBackupClaim(claim);
        if (backupClaim == null) {
            final ContentBlock content = claimMap.remove(claim);

            if (content == null) {
                logger.debug("Removed {} from repo but it did not exist", claim);
            } else {
                logger.debug("Removed {} from repo; Content = {}", claim, content);
                content.destroy();
            }
        } else {
            getBackupRepository().remove(backupClaim);
        }

        return true;
    }

    private boolean remove(final ResourceClaim claim) {
        if (claim == null) {
            return false;
        }

        final Set<ContentClaim> contentClaims = new HashSet<>();
        for (final Map.Entry<ContentClaim, ContentBlock> entry : claimMap.entrySet()) {
            final ContentClaim contentClaim = entry.getKey();
            if (contentClaim.getResourceClaim().equals(claim)) {
                contentClaims.add(contentClaim);
            }
        }

        boolean removed = false;
        for (final ContentClaim contentClaim : contentClaims) {
            if (remove(contentClaim)) {
                removed = true;
            }
        }

        return removed;
    }

    @Override
    public ContentClaim clone(final ContentClaim original, final boolean lossTolerant) throws IOException {
        final ContentClaim createdClaim = create(lossTolerant);

        try (final InputStream dataIn = read(original)) {
            final ContentRepository createdClaimRepo = lossTolerant ? this : getBackupRepository();
            if (createdClaimRepo == null) {
                throw new IllegalStateException("Cannot create non-loss-tolerant ContentClaim because there is no persistent Content Repository configured");
            }

            try (final OutputStream dataOut = createdClaimRepo.write(createdClaim)) {
                StreamUtils.copy(dataIn, dataOut);
            }
        }

        return createdClaim;
    }

    @Override
    public long merge(final Collection<ContentClaim> claims, final ContentClaim destination, final byte[] header, final byte[] footer, final byte[] demarcator) throws IOException {
        long bytes = 0L;

        try (final OutputStream out = write(destination)) {
            if (header != null) {
                out.write(header);
                bytes += header.length;
            }

            final Iterator<ContentClaim> itr = claims.iterator();
            while (itr.hasNext()) {
                final ContentClaim readClaim = itr.next();
                try (final InputStream in = read(readClaim)) {
                    bytes += StreamUtils.copy(in, out);
                }

                if (itr.hasNext() && demarcator != null) {
                    bytes += demarcator.length;
                    out.write(demarcator);
                }
            }

            if (footer != null) {
                bytes += footer.length;
                out.write(footer);
            }

            return bytes;
        }
    }

    @Override
    public long importFrom(final Path content, final ContentClaim claim) throws IOException {
        try (final InputStream in = new FileInputStream(content.toFile())) {
            return importFrom(in, claim);
        }
    }

    @Override
    public long importFrom(final InputStream in, final ContentClaim claim) throws IOException {
        final ContentClaim backupClaim = getBackupClaim(claim);
        if (backupClaim == null) {
            final ContentBlock content = getContent(claim);
            content.reset();

            return StreamUtils.copy(in, content.write());
        } else {
            return getBackupRepository().importFrom(in, claim);
        }
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append) throws IOException {
        return exportTo(claim, destination, append, 0L, size(claim));
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append, final long offset, final long length) throws IOException {
        if (claim == null) {
            if (append) {
                return 0L;
            }
            Files.createFile(destination);
            return 0L;
        }

        final StandardOpenOption openOption = append ? StandardOpenOption.APPEND : StandardOpenOption.CREATE;
        try (final InputStream in = read(claim);
                final OutputStream destinationStream = Files.newOutputStream(destination, openOption)) {

            if (offset > 0) {
                StreamUtils.skip(in, offset);
            }

            StreamUtils.copy(in, destinationStream, length);
            return length;
        }
    }

    @Override
    public long exportTo(ContentClaim claim, OutputStream destination) throws IOException {
        final InputStream in = read(claim);
        try {
            return StreamUtils.copy(in, destination);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    @Override
    public long exportTo(ContentClaim claim, OutputStream destination, long offset, long length) throws IOException {
        final InputStream in = read(claim);
        try {
            StreamUtils.skip(in, offset);
            StreamUtils.copy(in, destination, length);
        } finally {
            IOUtils.closeQuietly(in);
        }
        return length;
    }

    private ContentBlock getContent(final ContentClaim claim) throws ContentNotFoundException {
        final ContentBlock content = claimMap.get(claim);
        if (content == null) {
            throw new ContentNotFoundException(claim);
        }

        return content;
    }

    @Override
    public long size(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return 0L;
        }

        final ContentClaim backupClaim = getBackupClaim(claim);
        return backupClaim == null ? getContent(claim).getSize() : getBackupRepository().size(claim);
    }

    @Override
    public InputStream read(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        final ContentClaim backupClaim = getBackupClaim(claim);
        return backupClaim == null ? getContent(claim).read() : getBackupRepository().read(backupClaim);
    }

    @Override
    public OutputStream write(final ContentClaim claim) throws IOException {
        final ContentClaim backupClaim = getBackupClaim(claim);
        return backupClaim == null ? getContent(claim).write() : getBackupRepository().write(backupClaim);
    }

    @Override
    public void purge() {
        for (final ContentClaim claim : claimMap.keySet()) {
            claimManager.decrementClaimantCount(resolveClaim(claim).getResourceClaim());
            final ContentClaim backup = getBackupClaim(claim);
            if (backup != null) {
                getBackupRepository().remove(backup);
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public boolean isAccessible(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return false;
        }
        final ContentClaim backupClaim = getBackupClaim(claim);
        if (backupClaim == null) {
            final ContentBlock contentBlock = claimMap.get(claim);
            return contentBlock != null;
        }

        return getBackupRepository().isAccessible(backupClaim);
    }

    class ContentBlock {

        private final ClaimSwitchingOutputStream out;
        private final ContentClaim claim;
        private final AtomicLong repoSizeCounter;

        public ContentBlock(final ContentClaim claim, final AtomicLong repoSizeCounter) {
            this.claim = claim;
            this.repoSizeCounter = repoSizeCounter;

            out = new ClaimSwitchingOutputStream(new ArrayManagedOutputStream(memoryManager) {
                @Override
                public void write(int b) throws IOException {
                    try {
                        final long bufferLengthBefore = getBufferLength();
                        super.write(b);
                        final long bufferLengthAfter = getBufferLength();
                        final long bufferSpaceAdded = bufferLengthAfter - bufferLengthBefore;
                        if (bufferSpaceAdded > 0) {
                            repoSizeCounter.addAndGet(bufferSpaceAdded);
                        }
                    } catch (final IOException e) {
                        final byte[] buff = new byte[1];
                        buff[0] = (byte) (b & 0xFF);
                        redirect(buff, 0, 1);
                    }
                }

                @Override
                public void write(byte[] b) throws IOException {
                    write(b, 0, b.length);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    try {
                        final long bufferLengthBefore = getBufferLength();
                        super.write(b, off, len);
                        final long bufferLengthAfter = getBufferLength();
                        final long bufferSpaceAdded = bufferLengthAfter - bufferLengthBefore;
                        if (bufferSpaceAdded > 0) {
                            repoSizeCounter.addAndGet(bufferSpaceAdded);
                        }
                    } catch (final IOException e) {
                        redirect(b, off, len);
                    }
                }

                private void redirect(byte[] b, int off, int len) throws IOException {
                    logger.debug("Redirecting {}", claim);
                    out.redirect();
                    out.write(b, off, len);
                }
            });
        }

        public synchronized long getSize() throws IOException {
            return out.getSize();
        }

        public synchronized OutputStream write() {
            return out;
        }

        public synchronized InputStream read() throws IOException {
            return out.read();
        }

        public synchronized void reset() {
            out.reset();
        }

        public synchronized void destroy() {
            out.destroy();
        }

        private class ClaimSwitchingOutputStream extends FilterOutputStream {

            private ArrayManagedOutputStream amos;
            private OutputStream out;

            public ClaimSwitchingOutputStream(final ArrayManagedOutputStream out) {
                super(out);
                this.amos = out;
                this.out = out;
            }

            @Override
            public void write(final byte[] b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void write(final int b) throws IOException {
                out.write(b);
            }

            public void destroy() {
                final int vcosLength = amos.getBufferLength();
                amos.destroy();
                amos = null;
                repoSizeCounter.addAndGet(-vcosLength);
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                out.close();
            }

            public void reset() {
                amos.reset();
            }

            private void redirect() throws IOException {
                final ContentRepository backupRepo = getBackupRepository();
                if (backupRepo == null) {
                    throw new IOException("Content Repository is out of space");
                }

                final ContentClaim backupClaim = backupRepo.create(true);
                backupRepoClaimMap.put(claim, backupClaim);

                out = backupRepo.write(backupClaim);

                amos.writeTo(out);
                amos.destroy();
                amos = null;
            }

            public long getSize() throws IOException {
                if (amos == null) {
                    final ContentClaim backupClaim = getBackupClaim(claim);
                    return getBackupRepository().size(backupClaim);
                } else {
                    return amos.size();
                }
            }

            public InputStream read() throws IOException {
                if (amos == null) {
                    final ContentClaim backupClaim = getBackupClaim(claim);
                    return getBackupRepository().read(backupClaim);
                } else {
                    return amos.newInputStream();
                }
            }
        }
    }

    private class CleanupOldClaims implements Runnable {

        @Override
        public void run() {
            final List<ResourceClaim> destructable = new ArrayList<>(1000);
            while (true) {
                destructable.clear();
                claimManager.drainDestructableClaims(destructable, 1000, 5, TimeUnit.SECONDS);
                if (destructable.isEmpty()) {
                    return;
                }

                for (final ResourceClaim claim : destructable) {
                    remove(claim);
                }
            }
        }
    }
}
