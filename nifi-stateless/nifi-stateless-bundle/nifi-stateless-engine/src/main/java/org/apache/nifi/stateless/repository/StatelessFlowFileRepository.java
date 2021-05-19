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

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.QueueProvider;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class StatelessFlowFileRepository implements FlowFileRepository {
    private final AtomicLong sequenceCounter = new AtomicLong(0L);
    private ResourceClaimManager claimManager; // effectively final
    private volatile long maxId = 0L;

    @Override
    public void initialize(final ResourceClaimManager claimManager) {
        this.claimManager = claimManager;
    }

    @Override
    public long getStorageCapacity() {
        return 0;
    }

    @Override
    public long getUsableStorageSpace() {
        return 0;
    }

    @Override
    public String getFileStoreName() {
        return null;
    }

    @Override
    public void updateRepository(final Collection<RepositoryRecord> records) {
        records.forEach(this::updateClaimCounts);
    }

    private void updateClaimCounts(final RepositoryRecord record) {
        final ContentClaim currentClaim = record.getCurrentClaim();
        final ContentClaim originalClaim = record.getOriginalClaim();

        if (record.getType() == RepositoryRecordType.DELETE || record.getType() == RepositoryRecordType.CONTENTMISSING) {
            decrementClaimCount(currentClaim);
        }

        if (record.isContentModified()) {
            // records which have been updated - remove original if exists
            decrementClaimCount(originalClaim);
        }
    }

    private void decrementClaimCount(final ContentClaim claim) {
        if (claim == null) {
            return;
        }

        claimManager.decrementClaimantCount(claim.getResourceClaim());
    }

    @Override
    public long loadFlowFiles(final QueueProvider queueProvider) {
        return 0;
    }

    @Override
    public Set<String> findQueuesWithFlowFiles(final FlowFileSwapManager flowFileSwapManager) {
        throw new UnsupportedOperationException("Finding Queues that contain FlowFiles is not supported in Stateless NiFi");
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public long getNextFlowFileSequence() {
        return sequenceCounter.getAndIncrement();
    }

    @Override
    public long getMaxFlowFileIdentifier() {
        return maxId;
    }

    @Override
    public void updateMaxFlowFileIdentifier(final long maxId) {
        this.maxId = maxId;
    }

    @Override
    public void swapFlowFilesOut(final List<FlowFileRecord> swappedOut, final FlowFileQueue flowFileQueue, final String swapLocation) {
        throw new UnsupportedOperationException("Swapping is not supported");
    }

    @Override
    public void swapFlowFilesIn(final String swapLocation, final List<FlowFileRecord> flowFileRecords, final FlowFileQueue flowFileQueue) {
        throw new UnsupportedOperationException("Swapping is not supported");
    }

    @Override
    public boolean isValidSwapLocationSuffix(final String swapLocationSuffix) {
        return false;
    }

    @Override
    public void close() {

    }
}
