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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Set;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;

public class NonPurgeableContentRepository implements ContentRepository {
    private final ContentRepository delegate;

    public NonPurgeableContentRepository(final ContentRepository delegate) {
        this.delegate = delegate;
    }

    @Override
    public void initialize(final ContentRepositoryContext context) throws IOException {
        // No-op
    }

    @Override
    public void shutdown() {
        // No-op
    }

    @Override
    public Set<String> getContainerNames() {
        return delegate.getContainerNames();
    }

    @Override
    public long getContainerCapacity(final String containerName) throws IOException {
        return delegate.getContainerCapacity(containerName);
    }

    @Override
    public long getContainerUsableSpace(final String containerName) throws IOException {
        return delegate.getContainerUsableSpace(containerName);
    }

    @Override
    public String getContainerFileStoreName(final String containerName) {
        return delegate.getContainerFileStoreName(containerName);
    }

    @Override
    public ContentClaim create(final boolean lossTolerant) throws IOException {
        return delegate.create(lossTolerant);
    }

    @Override
    public int incrementClaimaintCount(final ContentClaim claim) {
        return delegate.incrementClaimaintCount(claim);
    }

    @Override
    public int getClaimantCount(final ContentClaim claim) {
        return delegate.getClaimantCount(claim);
    }

    @Override
    public int decrementClaimantCount(final ContentClaim claim) {
        return delegate.decrementClaimantCount(claim);
    }

    @Override
    public boolean remove(final ContentClaim claim) {
        return delegate.remove(claim);
    }

    @Override
    public ContentClaim clone(final ContentClaim original, final boolean lossTolerant) throws IOException {
        return delegate.clone(original, lossTolerant);
    }

    @Override
    public long importFrom(final Path content, final ContentClaim claim) throws IOException {
        return delegate.importFrom(content, claim);
    }

    @Override
    public long importFrom(final InputStream content, final ContentClaim claim) throws IOException {
        return delegate.importFrom(content, claim);
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append) throws IOException {
        return delegate.exportTo(claim, destination, append);
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append, final long offset, final long length) throws IOException {
        return delegate.exportTo(claim, destination, append, offset, length);
    }

    @Override
    public long exportTo(final ContentClaim claim, final OutputStream destination) throws IOException {
        return delegate.exportTo(claim, destination);
    }

    @Override
    public long exportTo(final ContentClaim claim, final OutputStream destination, final long offset, final long length) throws IOException {
        return delegate.exportTo(claim, destination, offset, length);
    }

    @Override
    public long size(final ContentClaim claim) throws IOException {
        return delegate.size(claim);
    }

    @Override
    public long size(final ResourceClaim claim) throws IOException {
        return delegate.size(claim);
    }

    @Override
    public InputStream read(final ContentClaim claim) throws IOException {
        return delegate.read(claim);
    }

    @Override
    public InputStream read(final ResourceClaim claim) throws IOException {
        return delegate.read(claim);
    }

    @Override
    public boolean isResourceClaimStreamSupported() {
        return delegate.isResourceClaimStreamSupported();
    }

    @Override
    public OutputStream write(final ContentClaim claim) throws IOException {
        return delegate.write(claim);
    }

    @Override
    public void purge() {
        // No-op
    }

    @Override
    public void cleanup() {
        // No-op
    }

    @Override
    public boolean isAccessible(final ContentClaim contentClaim) throws IOException {
        return delegate.isAccessible(contentClaim);
    }

    @Override
    public Set<ResourceClaim> getActiveResourceClaims(final String containerName) throws IOException {
        return delegate.getActiveResourceClaims(containerName);
    }

    @Override
    public boolean isActiveResourceClaimsSupported() {
        return delegate.isActiveResourceClaimsSupported();
    }
}
