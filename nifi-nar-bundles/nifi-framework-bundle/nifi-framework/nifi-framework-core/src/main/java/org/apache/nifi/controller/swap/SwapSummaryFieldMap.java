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

package org.apache.nifi.controller.swap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.schema.ResourceClaimFieldMap;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;

public class SwapSummaryFieldMap implements Record {
    private final SwapSummary swapSummary;
    private final RecordSchema schema;
    private final String queueIdentifier;
    private final Map<ResourceClaimFieldMap, Integer> claimCounts;

    public SwapSummaryFieldMap(final SwapSummary summary, final String queueIdentifier, final RecordSchema schema) {
        this.swapSummary = summary;
        this.queueIdentifier = queueIdentifier;
        this.schema = schema;

        final RecordField resourceClaimField = schema.getField(SwapSchema.RESOURCE_CLAIMS).getSubFields().get(0);
        final RecordSchema resourceClaimSchema = new RecordSchema(resourceClaimField.getSubFields());

        final List<ResourceClaim> resourceClaims = summary.getResourceClaims();
        claimCounts = new HashMap<>();
        for (final ResourceClaim claim : resourceClaims) {
            final ResourceClaimFieldMap fieldMap = new ResourceClaimFieldMap(claim, resourceClaimSchema);

            final Integer count = claimCounts.get(fieldMap);
            if (count == null) {
                claimCounts.put(fieldMap, 1);
            } else {
                claimCounts.put(fieldMap, count + 1);
            }
        }
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        switch (fieldName) {
            case SwapSchema.MAX_RECORD_ID:
                return swapSummary.getMaxFlowFileId();
            case SwapSchema.FLOWFILE_COUNT:
                return swapSummary.getQueueSize().getObjectCount();
            case SwapSchema.FLOWFILE_SIZE:
                return swapSummary.getQueueSize().getByteCount();
            case SwapSchema.QUEUE_IDENTIFIER:
                return queueIdentifier;
            case SwapSchema.RESOURCE_CLAIMS:
                return claimCounts;
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public static SwapSummary getSwapSummary(final Record record, final ResourceClaimManager claimManager) {
        final int flowFileCount = (Integer) record.getFieldValue(SwapSchema.FLOWFILE_COUNT);
        final long flowFileSize = (Long) record.getFieldValue(SwapSchema.FLOWFILE_SIZE);
        final QueueSize queueSize = new QueueSize(flowFileCount, flowFileSize);

        final long maxFlowFileId = (Long) record.getFieldValue(SwapSchema.MAX_RECORD_ID);

        final Map<Record, Integer> resourceClaimRecords = (Map<Record, Integer>) record.getFieldValue(SwapSchema.RESOURCE_CLAIMS);
        final List<ResourceClaim> resourceClaims = new ArrayList<>();
        for (final Map.Entry<Record, Integer> entry : resourceClaimRecords.entrySet()) {
            final Record resourceClaimRecord = entry.getKey();
            final ResourceClaim claim = ResourceClaimFieldMap.getResourceClaim(resourceClaimRecord, claimManager);

            for (int i = 0; i < entry.getValue(); i++) {
                resourceClaims.add(claim);
            }
        }

        return new StandardSwapSummary(queueSize, maxFlowFileId, resourceClaims);
    }
}
