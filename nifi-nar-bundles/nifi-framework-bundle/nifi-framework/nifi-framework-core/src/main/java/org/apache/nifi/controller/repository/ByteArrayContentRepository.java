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

import org.apache.nifi.controller.repository.claim.CachableContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;

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
import java.util.concurrent.atomic.AtomicLong;

public class ByteArrayContentRepository implements ContentRepository {
    private ResourceClaimManager resourceClaimManager;
    public static String CONTAINER_NAME = "in-memory";

    public static final String MAX_SIZE_PROPERTY = "nifi.bytearray.content.repository.max.size";
    private final long maxBytes;
    private final AtomicLong repoCurrentSize = new AtomicLong(0L);

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
    public void initialize(ContentRepositoryContext context) throws IOException {
        resourceClaimManager = context.getResourceClaimManager();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Set<String> getContainerNames() {
        return Collections.singleton(CONTAINER_NAME);
    }

    @Override
    public long getContainerCapacity(String containerName) throws IOException {
        return maxBytes;
    }

    @Override
    public long getContainerUsableSpace(String containerName) throws IOException {
        return maxBytes - repoCurrentSize.get();
    }

    @Override
    public String getContainerFileStoreName(String containerName) {
        return null;
    }

    @Override
    public ContentClaim create(boolean lossTolerant) throws IOException {
        // Loss Tolerance is not configurable
        // This is an in-memory repository without any backup
        final ContentClaim contentClaim = new ByteArrayContentClaim(repoCurrentSize, maxBytes);
        resourceClaimManager.incrementClaimantCount(contentClaim.getResourceClaim());

        return contentClaim;
    }

    @Override
    public int incrementClaimaintCount(ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return resourceClaimManager.incrementClaimantCount(resolveClaim(claim).getResourceClaim());
    }

    @Override
    public int getClaimantCount(ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return resourceClaimManager.getClaimantCount(resolveClaim(claim).getResourceClaim());
    }

    @Override
    public int decrementClaimantCount(ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return resourceClaimManager.decrementClaimantCount(resolveClaim(claim).getResourceClaim());
    }

    @Override
    public boolean remove(ContentClaim claim) {
        final ByteArrayContentClaim byteArrayContentClaim = verifyContentClaim(claim);
        final long claimLength = byteArrayContentClaim.getLength();
        if(byteArrayContentClaim.delete()) {
            repoCurrentSize.addAndGet(-claimLength);
            return true;
        }
        return false;
    }

    @Override
    public ContentClaim clone(ContentClaim original, boolean lossTolerant) throws IOException {
        final ContentClaim clone = create(lossTolerant);
        try (final InputStream in = read(original);
                final OutputStream out = write(clone)) {
            StreamUtils.copy(in, out);
        }

        return clone;
    }

    @Override
    public long merge(Collection<ContentClaim> claims, ContentClaim destination, byte[] header, byte[] footer,
            byte[] demarcator) throws IOException {
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
    public long importFrom(Path content, ContentClaim claim) throws IOException {
        try (final InputStream in = Files.newInputStream(content, StandardOpenOption.READ)) {
            return importFrom(in, claim);
        }
    }

    @Override
    public long importFrom(InputStream content, ContentClaim claim) throws IOException {
        try (final OutputStream out = write(claim)) {
            return StreamUtils.copy(content, out);
        }
    }

    @Override
    public long exportTo(ContentClaim claim, Path destination, boolean append) throws IOException {
        final OpenOption[] openOptions = append
                ? new StandardOpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.APPEND }
                : new StandardOpenOption[] { StandardOpenOption.CREATE };

        try (final OutputStream out = Files.newOutputStream(destination, openOptions)) {
            return exportTo(claim, out);
        }
    }

    @Override
    public long exportTo(ContentClaim claim, Path destination, boolean append, long offset, long length)
            throws IOException {
        final OpenOption[] openOptions = append
                ? new StandardOpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.APPEND }
                : new StandardOpenOption[] { StandardOpenOption.CREATE };

        try (final OutputStream out = Files.newOutputStream(destination, openOptions)) {
            return exportTo(claim, out, offset, length);
        }
    }

    @Override
    public long exportTo(ContentClaim claim, OutputStream destination) throws IOException {
        try (final InputStream in = read(claim)) {
            return StreamUtils.copy(in, destination);
        }
    }

    @Override
    public long exportTo(ContentClaim claim, OutputStream destination, long offset, long length) throws IOException {
        try (final InputStream in = read(claim)) {
            StreamUtils.skip(in, offset);
            StreamUtils.copy(in, destination, length);
        }

        return length;
    }

    @Override
    public long size(ContentClaim claim) throws IOException {
        return claim.getLength();
    }

    @Override
    public long size(ResourceClaim claim) throws IOException {
        return verifyResourceClaim(claim).getLength();
    }

    @Override
    public InputStream read(ContentClaim claim) throws IOException {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        final ByteArrayContentClaim byteArrayContentClaim = verifyContentClaim(claim);
        return byteArrayContentClaim.read();
    }

    @Override
    public InputStream read(ResourceClaim claim) throws IOException {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        final ByteArrayResourceClaim byteArrayResourceClaim = verifyResourceClaim(claim);
        return byteArrayResourceClaim.read();
    }

    @Override
    public OutputStream write(ContentClaim claim) throws IOException {
        final ByteArrayContentClaim byteArrayContentClaim = verifyContentClaim(claim);

        // repoCurrentSize.addAndGet(claim.getLength());
        return byteArrayContentClaim.writeTo();
    }

    private CachableContentClaim resolveClaim(final ContentClaim claim) {
        if (!(claim instanceof CachableContentClaim)) {
            throw new IllegalArgumentException("Cannot increment ClaimantCount of " + claim + " because it does not belong to this ContentRepository");
        }

        return (CachableContentClaim) claim;
    }

    private ByteArrayContentClaim verifyContentClaim(final ContentClaim claim) {
        Objects.requireNonNull(claim);
        if (!(claim instanceof ByteArrayContentClaim)) {
            throw new IllegalArgumentException("Cannot access Content Claim " + claim
                    + " because the Content Claim does not belong to this Content Repository");
        }

        return (ByteArrayContentClaim) claim;
    }

    private ByteArrayResourceClaim verifyResourceClaim(final ResourceClaim claim) {
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

    @Override
    public boolean isAccessible(ContentClaim contentClaim) throws IOException {
        return false;
    }

    public byte[] getBytes(final ContentClaim contentClaim) {
        final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
        if (!(resourceClaim instanceof ByteArrayResourceClaim)) {
            throw new IllegalArgumentException("Given ContentClaim was not created by this Repository");
        }

        return ((ByteArrayResourceClaim) resourceClaim).contents;
    }

    private static class ByteArrayContentClaim implements CachableContentClaim {
        private final ByteArrayResourceClaim resourceClaim;

        ByteArrayContentClaim(AtomicLong repoSize, long maxSize) {
            this.resourceClaim = new ByteArrayResourceClaim(repoSize, maxSize);
        }

        @Override
        public int compareTo(ContentClaim arg0) {
            return resourceClaim.compareTo(arg0.getResourceClaim());
        }

        @Override
        public ResourceClaim getResourceClaim() {
            return (ResourceClaim) resourceClaim;
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
            return "ByteArrayContentClaim [resourceClaim=" + resourceClaim + "]";
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
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
    
            if (obj == null) {
                return false;
            }
    
            final ContentClaim other = (ContentClaim) obj;
            if (!(obj instanceof ContentClaim)) {
                return false;
            }
    
            return resourceClaim.equals(other.getResourceClaim());
        }

        @Override
        public void setLength(long length) {
            // length managed by the ByteArrayResourceClaim
        }

    }

    public static class ByteArrayResourceClaim implements ResourceClaim {
        private byte[] contents;

        private static final AtomicLong idCounter = new AtomicLong(0L);
        private final String id = String.valueOf(idCounter.getAndIncrement());

        private AtomicLong repoSize;
        private AtomicLong managedSize;
        private long maxSize;

        ByteArrayResourceClaim(AtomicLong repoSize, long maxSize) {
            this.repoSize = repoSize;
            this.maxSize = maxSize;
            this.managedSize = new AtomicLong(0L);
        }

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
            return "section";
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
            return contents == null;
        }

        public long getLength() {
            return contents == null ? managedSize.get() : contents.length;
        }

        public OutputStream writeTo() {
            return new ManagedByteArrayOutputStream(repoSize, managedSize, maxSize);
        }

        public InputStream read() {
            if (contents == null) {
                return new ByteArrayInputStream(new byte[0]);
            }

            return new ByteArrayInputStream(contents);
        }

        public boolean delete() {
            contents = new byte[0];
            return true;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
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
            return "ByteArrayResourceClaim [container=" + getContainer() + ", section=" + getSection() + ", id=" + id + ", length=" + getLength() + "]";
        }

        class ManagedByteArrayOutputStream extends ByteArrayOutputStream {
            private AtomicLong repoSize;
            private AtomicLong managedSize;
            private long totalSize;
        
            ManagedByteArrayOutputStream(AtomicLong repoSize, AtomicLong managedSize, long totalSize) {
                this.repoSize = repoSize;
                this.managedSize = managedSize;
                this.totalSize = totalSize;
            }

            @Override
            public synchronized void write(int b) {
                if (repoSize.get() + 1 > totalSize) {
                    throw new OutOfMemoryError("Content of length 1 is too large to be written. Space left: " + (totalSize - repoSize.get()) + " bytes.");
                }
                super.write(b);

                repoSize.incrementAndGet();
                managedSize.incrementAndGet();
            }
        
            @Override
            public void write(byte[] b) throws IOException {
                if (repoSize.get() + b.length > totalSize) {
                    throw new OutOfMemoryError("Content of length "+ b.length + " is too large to be written. Space left: " + (totalSize - repoSize.get()) + " bytes.");
                }
                System.out.println(b.length);

                super.write(b);

                repoSize.addAndGet(b.length);
                managedSize.addAndGet(b.length);
            }
            
            @Override
            public synchronized void reset() {
                long currentSize = this.size();
                super.reset();
                repoSize.addAndGet(-currentSize);
                managedSize.set(0);
            }
        
            @Override
            public void close() throws IOException {
                super.close();
                ByteArrayResourceClaim.this.contents = toByteArray();
            }
        }
    }
}
