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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.controller.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaimManager;

/**
 * <p>
 * An in-memory implementation of the {@link FlowFileRepository}. Upon restart,
 * all FlowFiles will be discarded, including those that have been swapped out
 * by a {@link FlowFileSwapManager}.
 * </p>
 */
public class VolatileFlowFileRepository implements FlowFileRepository {

    private final AtomicLong idGenerator = new AtomicLong(0L);
    private ContentClaimManager claimManager;	// effectively final

    @Override
    public void initialize(final ContentClaimManager claimManager) {
        this.claimManager = claimManager;
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public long getStorageCapacity() throws IOException {
        return 1L;
    }

    @Override
    public long getUsableStorageSpace() throws IOException {
        return 0L;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void updateRepository(final Collection<RepositoryRecord> records) throws IOException {
        for (final RepositoryRecord record : records) {
            if (record.getType() == RepositoryRecordType.DELETE) {
                // For any DELETE record that we have, if current claim's claimant count <= 0, mark it as destructable
                if (record.getCurrentClaim() != null && claimManager.getClaimantCount(record.getCurrentClaim()) <= 0) {
                    claimManager.markDestructable(record.getCurrentClaim());
                }

                // If the original claim is different than the current claim and the original claim has a claimant count <= 0, mark it as destructable.
                if (record.getOriginalClaim() != null && !record.getOriginalClaim().equals(record.getCurrentClaim()) && claimManager.getClaimantCount(record.getOriginalClaim()) <= 0) {
                    claimManager.markDestructable(record.getOriginalClaim());
                }
            } else if (record.getType() == RepositoryRecordType.UPDATE) {
                // if we have an update, and the original is no longer needed, mark original as destructable
                if (record.getOriginalClaim() != null && record.getCurrentClaim() != record.getOriginalClaim() && claimManager.getClaimantCount(record.getOriginalClaim()) <= 0) {
                    claimManager.markDestructable(record.getOriginalClaim());
                }
            }
        }
    }

    @Override
    public long loadFlowFiles(final QueueProvider queueProvider, final long minimumSequenceNumber) throws IOException {
        idGenerator.set(minimumSequenceNumber);
        return 0;
    }

    @Override
    public long getNextFlowFileSequence() {
        return idGenerator.getAndIncrement();
    }

    @Override
    public long getMaxFlowFileIdentifier() throws IOException {
        return idGenerator.get() - 1;
    }

    @Override
    public void swapFlowFilesIn(String swapLocation, List<FlowFileRecord> flowFileRecords, FlowFileQueue queue) throws IOException {
    }

    @Override
    public void swapFlowFilesOut(List<FlowFileRecord> swappedOut, FlowFileQueue queue, String swapLocation) throws IOException {
    }

}
