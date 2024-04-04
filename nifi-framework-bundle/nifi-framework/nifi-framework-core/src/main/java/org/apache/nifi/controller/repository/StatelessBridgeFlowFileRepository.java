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
import org.apache.nifi.stateless.repository.StatelessFlowFileRepository;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A FlowFile Repository that bridges between the Stateless FlowFile Repository and the NiFi instance's FlowFile Repository
 */
public class StatelessBridgeFlowFileRepository extends StatelessFlowFileRepository {
    private final FlowFileRepository nifiFlowFileRepository;
    private final ResourceClaimManager resourceClaimManager;

    public StatelessBridgeFlowFileRepository(final FlowFileRepository nifiFlowFileRepository, final ResourceClaimManager resourceClaimManager) {
        this.nifiFlowFileRepository = nifiFlowFileRepository;
        this.resourceClaimManager = resourceClaimManager;
    }


    @Override
    public long getNextFlowFileSequence() {
        return nifiFlowFileRepository.getNextFlowFileSequence();
    }

    @Override
    public void updateRepository(final Collection<RepositoryRecord> records) throws IOException {
        super.updateRepository(records);

        final Set<ContentClaim> transientClaims = new HashSet<>();
        for (final RepositoryRecord record : records) {
            final RepositoryRecordType type = record.getType();
            if (type == RepositoryRecordType.DELETE || type == RepositoryRecordType.CONTENTMISSING) {
                final ContentClaim contentClaim = record.getCurrentClaim();
                if (isDestructable(contentClaim)) {
                    transientClaims.add(contentClaim);
                }
            }

            if (record.isContentModified()) {
                final ContentClaim contentClaim = record.getOriginalClaim();
                if (isDestructable(contentClaim)) {
                    transientClaims.add(contentClaim);
                }
            }
        }

        if (!transientClaims.isEmpty()) {
            nifiFlowFileRepository.updateRepository(Collections.singletonList(new StandardRepositoryRecord(transientClaims)));
        }
    }

    private boolean isDestructable(final ContentClaim contentClaim) {
        if (contentClaim == null) {
            return false;
        }

        final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
        if (resourceClaim.isInUse()) {
            return false;
        }

        final int claimantCount = resourceClaimManager.getClaimantCount(resourceClaim);
        return claimantCount == 0;
    }
}
