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
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Set;

/**
 * A {@link ContentRepository} for an embedded Stateless Process Group that defers the choice of backing repository until it is first used. This is necessary
 * because the {@link org.apache.nifi.groups.StatelessGroupNode} is created when the Process Group is constructed, which is before the Process Group's maximum
 * in-memory FlowFile content size has been configured. Resolving the backing repository lazily ensures the configured value is honored: when the maximum
 * in-memory FlowFile content size is greater than zero, a {@link SpillableContentRepository} buffers FlowFile content in memory up to that size and spills to
 * the NiFi instance's Content Repository once it is exceeded; when the size is zero, the NiFi instance's Content Repository is used directly.
 */
public class DeferredStatelessContentRepository implements ContentRepository {
    private final ProcessGroup processGroup;
    private final ContentRepository contentRepositoryDelegate;
    private final FlowFileRepository nifiFlowFileRepository;
    private final ResourceClaimManager resourceClaimManager;
    private final EventReporter eventReporter;

    private volatile ContentRepository delegate;
    private volatile long resolvedMemoryThresholdBytes = Long.MIN_VALUE;

    public DeferredStatelessContentRepository(final ProcessGroup processGroup, final ContentRepository contentRepositoryDelegate, final FlowFileRepository nifiFlowFileRepository,
                                              final ResourceClaimManager resourceClaimManager, final EventReporter eventReporter) {
        this.processGroup = processGroup;
        this.contentRepositoryDelegate = contentRepositoryDelegate;
        this.nifiFlowFileRepository = nifiFlowFileRepository;
        this.resourceClaimManager = resourceClaimManager;
        this.eventReporter = eventReporter;
    }

    /**
     * Ensures that the content for the given claim is accessible outside of the Stateless Process Group and returns a Content Claim that references it in the
     * NiFi instance's Content Repository. When FlowFile content was buffered in memory, this writes it to the NiFi Content Repository and returns the new claim.
     * When content was not buffered in memory (or the Process Group is not configured to buffer content in memory), the claim is returned unchanged.
     *
     * @param claim the claim to make externally accessible
     * @return a Content Claim whose content is stored in the NiFi instance's Content Repository
     * @throws IOException if the content cannot be written to the NiFi Content Repository
     */
    public ContentClaim exportForExternalUse(final ContentClaim claim) throws IOException {
        final ContentRepository resolved = getDelegate();
        if (resolved instanceof final SpillableContentRepository spillableContentRepository) {
            return spillableContentRepository.exportForExternalUse(claim);
        }

        return claim;
    }

    /**
     * Completes the handoff of a prepared Content Claim after the NiFi FlowFile Repository has been updated successfully.
     *
     * @param claim the original claim passed to {@link #exportForExternalUse(ContentClaim)}
     */
    public void commitExportForExternalUse(final ContentClaim claim) {
        final ContentRepository resolved = getDelegate();
        if (resolved instanceof final SpillableContentRepository spillableContentRepository) {
            spillableContentRepository.commitExportForExternalUse(claim);
        }
    }

    @Override
    public void initialize(final ContentRepositoryContext context) {
        // The backing repository is initialized when it is resolved; nothing to do here.
    }

    @Override
    public void shutdown() {
        final ContentRepository resolved = delegate;
        if (resolved != null) {
            resolved.shutdown();
        }
    }

    @Override
    public void purge() {
        final ContentRepository resolved = delegate;
        if (resolved != null) {
            resolved.purge();
        }
    }

    @Override
    public void cleanup() {
        final ContentRepository resolved = delegate;
        if (resolved != null) {
            resolved.cleanup();
        }
    }

    @Override
    public Set<String> getContainerNames() {
        return getDelegate().getContainerNames();
    }

    @Override
    public long getContainerCapacity(final String containerName) throws IOException {
        return getDelegate().getContainerCapacity(containerName);
    }

    @Override
    public long getContainerUsableSpace(final String containerName) throws IOException {
        return getDelegate().getContainerUsableSpace(containerName);
    }

    @Override
    public String getContainerFileStoreName(final String containerName) {
        return getDelegate().getContainerFileStoreName(containerName);
    }

    @Override
    public ContentClaim create(final boolean lossTolerant) throws IOException {
        return getDelegate().create(lossTolerant);
    }

    @Override
    public int incrementClaimaintCount(final ContentClaim claim) {
        return getDelegate().incrementClaimaintCount(claim);
    }

    @Override
    public int getClaimantCount(final ContentClaim claim) {
        return getDelegate().getClaimantCount(claim);
    }

    @Override
    public int decrementClaimantCount(final ContentClaim claim) {
        return getDelegate().decrementClaimantCount(claim);
    }

    @Override
    public boolean remove(final ContentClaim claim) {
        return getDelegate().remove(claim);
    }

    @Override
    public ContentClaim clone(final ContentClaim original, final boolean lossTolerant) throws IOException {
        return getDelegate().clone(original, lossTolerant);
    }

    @Override
    public long importFrom(final Path content, final ContentClaim claim) throws IOException {
        return getDelegate().importFrom(content, claim);
    }

    @Override
    public long importFrom(final InputStream content, final ContentClaim claim) throws IOException {
        return getDelegate().importFrom(content, claim);
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append) throws IOException {
        return getDelegate().exportTo(claim, destination, append);
    }

    @Override
    public long exportTo(final ContentClaim claim, final Path destination, final boolean append, final long offset, final long length) throws IOException {
        return getDelegate().exportTo(claim, destination, append, offset, length);
    }

    @Override
    public long exportTo(final ContentClaim claim, final OutputStream destination) throws IOException {
        return getDelegate().exportTo(claim, destination);
    }

    @Override
    public long exportTo(final ContentClaim claim, final OutputStream destination, final long offset, final long length) throws IOException {
        return getDelegate().exportTo(claim, destination, offset, length);
    }

    @Override
    public long size(final ContentClaim claim) throws IOException {
        return getDelegate().size(claim);
    }

    @Override
    public long size(final ResourceClaim claim) throws IOException {
        return getDelegate().size(claim);
    }

    @Override
    public InputStream read(final ContentClaim claim) throws IOException {
        return getDelegate().read(claim);
    }

    @Override
    public InputStream read(final ResourceClaim claim) throws IOException {
        return getDelegate().read(claim);
    }

    @Override
    public boolean isResourceClaimStreamSupported() {
        return getDelegate().isResourceClaimStreamSupported();
    }

    @Override
    public OutputStream write(final ContentClaim claim) throws IOException {
        return getDelegate().write(claim);
    }

    @Override
    public boolean isAccessible(final ContentClaim contentClaim) throws IOException {
        return getDelegate().isAccessible(contentClaim);
    }

    private ContentRepository getDelegate() {
        long memoryThresholdBytes = processGroup.resolveStatelessContentMaxHeap();
        ContentRepository resolved = delegate;
        if (resolved != null && resolvedMemoryThresholdBytes == memoryThresholdBytes) {
            return resolved;
        }

        synchronized (this) {
            memoryThresholdBytes = processGroup.resolveStatelessContentMaxHeap();
            resolved = delegate;
            if (resolved == null || resolvedMemoryThresholdBytes != memoryThresholdBytes) {
                if (resolved != null) {
                    resolved.shutdown();
                }

                if (memoryThresholdBytes > 0) {
                    final SpillableContentRepository spillableContentRepository = new SpillableContentRepository(contentRepositoryDelegate, nifiFlowFileRepository, memoryThresholdBytes);
                    spillableContentRepository.initialize(new StandardContentRepositoryContext(resourceClaimManager, eventReporter));
                    resolved = spillableContentRepository;
                } else {
                    resolved = contentRepositoryDelegate;
                }

                resolvedMemoryThresholdBytes = memoryThresholdBytes;
                delegate = resolved;
            }

            return resolved;
        }
    }
}
