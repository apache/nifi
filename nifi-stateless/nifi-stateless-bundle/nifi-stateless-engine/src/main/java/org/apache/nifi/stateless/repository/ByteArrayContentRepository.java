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
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

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

    @Override
    public void initialize(final ResourceClaimManager claimManager) {
        resourceClaimManager = claimManager;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Set<String> getContainerNames() {
        return Collections.singleton("container");
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
        final ContentClaim contentClaim = new ByteArrayContentClaim();
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
    public InputStream read(final ContentClaim claim) {
        final ByteArrayContentClaim byteArrayContentClaim = verifyClaim(claim);
        return byteArrayContentClaim.read();
    }

    @Override
    public InputStream read(final ResourceClaim claim) throws IOException {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        if (!(claim instanceof ByteArrayResourceClaim)) {
            throw new IllegalArgumentException("Cannot access Resource Claim " + claim + " because the Resource Claim does not belong to this Content Repository");
        }

        final ByteArrayResourceClaim byteArrayResourceClaim = (ByteArrayResourceClaim) claim;
        return byteArrayResourceClaim.read();
    }

    @Override
    public OutputStream write(final ContentClaim claim) {
        final ByteArrayContentClaim byteArrayContentClaim = verifyClaim(claim);
        return byteArrayContentClaim.writeTo();
    }

    private ByteArrayContentClaim verifyClaim(final ContentClaim claim) {
        Objects.requireNonNull(claim);
        if (!(claim instanceof ByteArrayContentClaim)) {
            throw new IllegalArgumentException("Cannot access Content Claim " + claim + " because the Content Claim does not belong to this Content Repository");
        }

        return (ByteArrayContentClaim) claim;
    }

    @Override
    public void purge() {
    }

    @Override
    public void cleanup() {
    }

    public byte[] getBytes(final ContentClaim contentClaim) {
        final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
        if (!(resourceClaim instanceof ByteArrayResourceClaim)) {
            throw new IllegalArgumentException("Given ContentClaim was not created by this Repository");
        }

        return ((ByteArrayResourceClaim) resourceClaim).contents;
    }

    @Override
    public boolean isAccessible(final ContentClaim contentClaim) {
        return false;
    }

    private static class ByteArrayContentClaim implements ContentClaim {
        private final ByteArrayResourceClaim resourceClaim = new ByteArrayResourceClaim();

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

        @Override
        public int compareTo(final ContentClaim o) {
            return resourceClaim.compareTo(o.getResourceClaim());
        }

        public OutputStream writeTo() {
            return resourceClaim.writeTo();
        }

        public InputStream read() {
            return resourceClaim.read();
        }
    }

    private static class ByteArrayResourceClaim implements ResourceClaim {
        private static final AtomicLong idCounter = new AtomicLong(0L);
        private final String id = String.valueOf(idCounter.getAndIncrement());
        private byte[] contents;

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String getContainer() {
            return "container";
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
            return true;
        }

        public long getLength() {
            return contents == null ? 0L : contents.length;
        }

        public OutputStream writeTo() {
            return new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    super.close();
                    ByteArrayResourceClaim.this.contents = toByteArray();
                }
            };
        }

        public InputStream read() {
            if (contents == null) {
                return new ByteArrayInputStream(new byte[0]);
            }

            return new ByteArrayInputStream(contents);
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
    }
}
