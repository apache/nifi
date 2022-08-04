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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.engine.FlowEngine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayContentRepository implements ContentRepository {
    private static final Logger logger = LoggerFactory.getLogger(ByteArrayContentRepository.class);
    private final ScheduledExecutorService executor = new FlowEngine(3, "ByteArrayContentRepository Cleaning Workers", true);

    private ResourceClaimManager resourceClaimManager;
    public static final String MAX_SIZE_PROPERTY = "nifi.bytearray.content.repository.max.size";
    public static final String CONTAINER_NAME = "in-memory";
    public static final String SECTION_NAME = "in-memory";

    private final AtomicLong repoCurrentSize = new AtomicLong(0L);
    private final AtomicLong idCounter = new AtomicLong(0L);
    private final long maxBytes;

    public ByteArrayContentRepository() {
        maxBytes = 0L;
    }

    public ByteArrayContentRepository(final NiFiProperties nifiProperties) {
        final String maxSizeProperty = nifiProperties.getProperty(MAX_SIZE_PROPERTY);
        if (maxSizeProperty == null) {
            maxBytes = (long) DataUnit.B.convert(100D, DataUnit.MB);
        } else {
            maxBytes = DataUnit.parseDataSize(maxSizeProperty, DataUnit.B).longValue();
        }
    }
    @Override
    public void initialize(final ContentRepositoryContext context) throws IOException {
        resourceClaimManager = context.getResourceClaimManager();

        for (int i = 0; i < 3; i++) {
            executor.scheduleWithFixedDelay(new CleanupOldClaims(), 1000, 10, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Set<String> getContainerNames() {
        return Collections.singleton(CONTAINER_NAME);
    }

    @Override
    public long getContainerCapacity(final String containerName) {
        return maxBytes;
    }

    @Override
    public long getContainerUsableSpace(final String containerName) {
        final long usableSpace = maxBytes - repoCurrentSize.get();
        logger.trace("Usable Space of container "+containerName+" is " + usableSpace);
        return usableSpace;
    }

    @Override
    public String getContainerFileStoreName(final String containerName) {
        return CONTAINER_NAME;
    }

    @Override
    public ContentClaim create(final boolean lossTolerant) throws IOException {
        // Loss Tolerance is not configurable
        // This is an in-memory repository without any backup
        final ContentClaim contentClaim = new ByteArrayContentClaim();
        resourceClaimManager.incrementClaimantCount(contentClaim.getResourceClaim());
        logger.trace("A ContentClaim has been created : " + contentClaim.toString());
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
        final ByteArrayContentClaim byteArrayContentClaim = verifyContentClaim(claim);
        return byteArrayContentClaim.delete();
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
    public long merge(final Collection<ContentClaim> claims, final ContentClaim destination, final byte[] header, final byte[] footer, final byte[] demarcator) throws IOException {
        if (claims.contains(destination)) {
            throw new IllegalArgumentException("destination cannot be within claims");
        }

        try (final ByteCountingOutputStream out = new ByteCountingOutputStream(write(destination))) {
            if (header != null) {
                out.write(header);
            }

            int i = 0;
            for (final ContentClaim claim : claims) {
                try (final InputStream in = read(claim)) {
                    StreamUtils.copy(in, out);
                }

                if (++i < claims.size() && demarcator != null) {
                    out.write(demarcator);
                }
            }

            if (footer != null) {
                out.write(footer);
            }

            return out.getBytesWritten();
        }
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
        return verifyResourceClaim(claim).getLength();
    }

    @Override
    public InputStream read(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        final ByteArrayContentClaim byteArrayContentClaim = verifyContentClaim(claim);
        return byteArrayContentClaim.read();
    }

    @Override
    public InputStream read(final ResourceClaim claim) {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        final ByteArrayResourceClaim byteArrayResourceClaim = verifyResourceClaim(claim);
        return byteArrayResourceClaim.read();
    }

    @Override
    public OutputStream write(final ContentClaim claim) {
        final ByteArrayContentClaim byteArrayContentClaim = verifyContentClaim(claim);
        return byteArrayContentClaim.writeTo();
    }

    protected static ByteArrayContentClaim verifyContentClaim(final ContentClaim claim) {
        Objects.requireNonNull(claim);
        if (!(claim instanceof ByteArrayContentClaim)) {
            throw new IllegalArgumentException("Cannot access Content Claim " + claim
                    + " because the Content Claim does not belong to this Content Repository");
        }

        return (ByteArrayContentClaim) claim;
    }

    protected static ByteArrayResourceClaim verifyResourceClaim(final ResourceClaim claim) {
        Objects.requireNonNull(claim);
        if (!(claim instanceof ByteArrayResourceClaim)) {
            throw new IllegalArgumentException("Cannot access Resouce Claim " + claim
                    + " because the Resouce Claim does not belong to this Content Repository");
        }

        return (ByteArrayResourceClaim) claim;
    }

    @Override
    public void purge() {
        resourceClaimManager.purge();
    }

    @Override
    public void cleanup() {
    }

    public byte[] getBytes(final ContentClaim contentClaim) {
        final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
        return verifyResourceClaim(resourceClaim).contents;
    }

    @Override
    public boolean isAccessible(final ContentClaim contentClaim) {
        return false;
    }

    protected class ByteArrayContentClaim implements ContentClaim {
        private final ByteArrayResourceClaim resourceClaim = new ByteArrayResourceClaim();

        @Override
        public int compareTo(final ContentClaim arg0) {
            return resourceClaim.compareTo(arg0.getResourceClaim());
        }

        @Override
        public void setLength(long length) {
            // do nothing
        }

        @Override
        public ResourceClaim getResourceClaim() {
            return resourceClaim;
        }

        @Override
        public long getOffset() {
            return 0;
        }

        @Override
        public long getLength() {
            return resourceClaim.getLength();
        }

        public OutputStream writeTo() {
            return resourceClaim.writeTo();
        }

        public InputStream read() {
            return resourceClaim.read();
        }

        public boolean delete() {
            return resourceClaim.delete();
        }

        @Override
        public String toString() {
            return "ByteArrayContentClaim[resourceClaim=" + resourceClaim.toString() + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 37;
            int result = 1;
            result = prime * result;
            result = prime * result + (resourceClaim == null ? 0 : resourceClaim.hashCode());
            return result;
        }
    
        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }

            try {
                return resourceClaim.equals(verifyContentClaim((ContentClaim) other).getResourceClaim());
            } catch (Exception e) {
                return false;
            }
        }
    }

    protected class ByteArrayResourceClaim implements ResourceClaim {
        private byte[] contents;
        private final String id = String.valueOf(idCounter.getAndIncrement());
        private AtomicLong internalSize = new AtomicLong(0);

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String getContainer() {
            return CONTAINER_NAME;
        }

        @Override
        public String getSection() {
            return SECTION_NAME;
        }

        @Override
        public boolean isLossTolerant() {
            return true;
        }

        @Override
        public boolean isWritable() {
            return contents == null;
        }

        @Override
        public boolean isInUse() {
            return true;
        }

        public long getLength() {
            return internalSize.get();
        }

        public OutputStream writeTo() {
            return new ManagedByteArrayOutputStream();
        }

        public InputStream read() {
            if (contents == null) {
                return new ByteArrayInputStream(new byte[0]);
            }

            return new ByteArrayInputStream(contents);
        }

        public boolean delete() {
            repoCurrentSize.addAndGet(-internalSize.get());
            internalSize.lazySet(0L);
            contents = null;      
            return true;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()){
                return false;
            }

            final ByteArrayResourceClaim that = (ByteArrayResourceClaim) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public String toString() {
            return "ByteArrayResourceClaim[ container=" + getContainer() + ", section=" + getSection() + ", id=" + id + ", length=" + getLength() + " ]";
        }

        class ManagedByteArrayOutputStream extends ByteArrayOutputStream {

            @Override
            public synchronized void write(int b) {
                if (ByteArrayContentRepository.this.repoCurrentSize.get() + 1 > ByteArrayContentRepository.this.maxBytes) {
                    throw new OutOfMemoryError("Content repository is out of space");
                }

                try {
                    super.write(b);
                }
                finally {
                    ByteArrayContentRepository.this.repoCurrentSize.incrementAndGet();
                    internalSize.incrementAndGet();
                }
            }
        
            @Override
            public void write(byte[] b, int off, int len) {
                if (ByteArrayContentRepository.this.repoCurrentSize.get() + len > ByteArrayContentRepository.this.maxBytes) {
                    throw new OutOfMemoryError("Content repository is out of space");                }

                try {
                    super.write(b, off, len);
                }
                finally {
                    ByteArrayContentRepository.this.repoCurrentSize.addAndGet(len);
                    internalSize.addAndGet(len);
                }
            }
        
            @Override
            public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
            }
            
            @Override
            public synchronized void reset() {
                super.reset();
                repoCurrentSize.addAndGet(-internalSize.get());
                internalSize.lazySet(0L);
                contents = null;
            }
        
            @Override
            public void close() throws IOException {
                super.close();
                ByteArrayResourceClaim.this.contents = toByteArray();
            }
        }
    }

    private class CleanupOldClaims implements Runnable {

        @Override
        public void run() {
            final List<ResourceClaim> destructable = new ArrayList<>(1000);
            while (true) {
                destructable.clear();
                resourceClaimManager.drainDestructableClaims(destructable, 1000, 5, TimeUnit.SECONDS);
                if (destructable.isEmpty()) {
                    return;
                }

                for (final ResourceClaim resourceClaim : destructable) {
                    verifyResourceClaim(resourceClaim).delete();
                }
            }
        }
    }
}