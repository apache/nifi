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

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SerializedRepositoryRecord;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;

public class RepositoryRecordFieldMap implements Record {
    private final SerializedRepositoryRecord record;
    private final FlowFileRecord flowFile;
    private final RecordSchema schema;
    private final RecordSchema contentClaimSchema;

    public RepositoryRecordFieldMap(final SerializedRepositoryRecord record, final RecordSchema repoRecordSchema, final RecordSchema contentClaimSchema) {
        this.schema = repoRecordSchema;
        this.contentClaimSchema = contentClaimSchema;
        this.record = record;
        this.flowFile = record.getFlowFileRecord();
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        return switch (fieldName) {
            case RepositoryRecordSchema.ACTION_TYPE -> record.getType().name();
            case RepositoryRecordSchema.RECORD_ID -> record.getFlowFileRecord().getId();
            case RepositoryRecordSchema.SWAP_LOCATION -> record.getSwapLocation();
            case FlowFileSchema.ATTRIBUTES -> flowFile.getAttributes();
            case FlowFileSchema.ENTRY_DATE -> flowFile.getEntryDate();
            case FlowFileSchema.FLOWFILE_SIZE -> flowFile.getSize();
            case FlowFileSchema.LINEAGE_START_DATE -> flowFile.getLineageStartDate();
            case FlowFileSchema.LINEAGE_START_INDEX -> flowFile.getLineageStartIndex();
            case FlowFileSchema.QUEUE_DATE -> flowFile.getLastQueueDate();
            case FlowFileSchema.QUEUE_DATE_INDEX -> flowFile.getQueueDateIndex();
            case FlowFileSchema.CONTENT_CLAIM -> {
                final ContentClaimFieldMap contentClaimFieldMap = record.getContentClaim() == null ? null
                        : new ContentClaimFieldMap(record.getContentClaim(), record.getClaimOffset(), contentClaimSchema);
                yield contentClaimFieldMap;
            }
            case RepositoryRecordSchema.QUEUE_IDENTIFIER -> record.getQueueIdentifier();
            default -> null;
        };
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
