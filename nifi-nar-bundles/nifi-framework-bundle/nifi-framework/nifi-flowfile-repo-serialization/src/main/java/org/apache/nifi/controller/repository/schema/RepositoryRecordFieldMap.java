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

package org.apache.nifi.controller.repository.schema;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;

public class RepositoryRecordFieldMap implements Record {
    private final RepositoryRecord record;
    private final FlowFileRecord flowFile;
    private final RecordSchema schema;
    private final RecordSchema contentClaimSchema;

    public RepositoryRecordFieldMap(final RepositoryRecord record, final RecordSchema repoRecordSchema, final RecordSchema contentClaimSchema) {
        this.schema = repoRecordSchema;
        this.contentClaimSchema = contentClaimSchema;
        this.record = record;
        this.flowFile = record.getCurrent();
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        switch (fieldName) {
            case RepositoryRecordSchema.ACTION_TYPE:
                return record.getType().name();
            case RepositoryRecordSchema.RECORD_ID:
                return record.getCurrent().getId();
            case RepositoryRecordSchema.SWAP_LOCATION:
                return record.getSwapLocation();
            case FlowFileSchema.ATTRIBUTES:
                return flowFile.getAttributes();
            case FlowFileSchema.ENTRY_DATE:
                return flowFile.getEntryDate();
            case FlowFileSchema.FLOWFILE_SIZE:
                return flowFile.getSize();
            case FlowFileSchema.LINEAGE_START_DATE:
                return flowFile.getLineageStartDate();
            case FlowFileSchema.LINEAGE_START_INDEX:
                return flowFile.getLineageStartIndex();
            case FlowFileSchema.QUEUE_DATE:
                return flowFile.getLastQueueDate();
            case FlowFileSchema.QUEUE_DATE_INDEX:
                return flowFile.getQueueDateIndex();
            case FlowFileSchema.CONTENT_CLAIM:
                final ContentClaimFieldMap contentClaimFieldMap = record.getCurrentClaim() == null ? null
                    : new ContentClaimFieldMap(record.getCurrentClaim(), record.getCurrentClaimOffset(), contentClaimSchema);
                return contentClaimFieldMap;
            case RepositoryRecordSchema.QUEUE_IDENTIFIER:
                final FlowFileQueue queue = record.getDestination() == null ? record.getOriginalQueue() : record.getDestination();
                return queue == null ? null : queue.getIdentifier();
            default:
                return null;
        }
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return "RepositoryRecordFieldMap[" + record + "]";
    }
}
