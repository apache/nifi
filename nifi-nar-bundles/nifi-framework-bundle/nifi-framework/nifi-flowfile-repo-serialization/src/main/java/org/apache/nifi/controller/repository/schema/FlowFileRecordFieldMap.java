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

import java.util.Map;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;

public class FlowFileRecordFieldMap implements Record {
    private final FlowFileRecord flowFile;
    private final RecordSchema schema;
    private final RecordSchema contentClaimSchema;
    private final ContentClaimFieldMap contentClaim;

    public FlowFileRecordFieldMap(final FlowFileRecord flowFile, final RecordSchema schema) {
        this.flowFile = flowFile;
        this.schema = schema;

        final RecordField contentClaimField = schema.getField(FlowFileSchema.CONTENT_CLAIM);
        contentClaimSchema = new RecordSchema(contentClaimField.getSubFields());
        contentClaim = flowFile.getContentClaim() == null ? null : new ContentClaimFieldMap(flowFile.getContentClaim(), flowFile.getContentClaimOffset(), contentClaimSchema);
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        switch (fieldName) {
            case FlowFileSchema.ATTRIBUTES:
                return flowFile.getAttributes();
            case FlowFileSchema.CONTENT_CLAIM:
                return contentClaim;
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
            case FlowFileSchema.RECORD_ID:
                return flowFile.getId();
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public static FlowFileRecord getFlowFile(final Record record, final ResourceClaimManager claimManager) {
        final StandardFlowFileRecord.Builder builder = new StandardFlowFileRecord.Builder();
        builder.id((Long) record.getFieldValue(FlowFileSchema.RECORD_ID));
        builder.entryDate((Long) record.getFieldValue(FlowFileSchema.ENTRY_DATE));
        builder.size((Long) record.getFieldValue(FlowFileSchema.FLOWFILE_SIZE));
        builder.addAttributes((Map<String, String>) record.getFieldValue(FlowFileSchema.ATTRIBUTES));
        builder.lineageStart((Long) record.getFieldValue(FlowFileSchema.LINEAGE_START_DATE), (Long) record.getFieldValue(FlowFileSchema.LINEAGE_START_INDEX));
        builder.lastQueued((Long) record.getFieldValue(FlowFileSchema.QUEUE_DATE), (Long) record.getFieldValue(FlowFileSchema.QUEUE_DATE_INDEX));

        final Record contentClaimRecord = (Record) record.getFieldValue(FlowFileSchema.CONTENT_CLAIM);
        if (contentClaimRecord != null) {
            final ContentClaim claim = ContentClaimFieldMap.getContentClaim(contentClaimRecord, claimManager);
            builder.contentClaim(claim);

            final Long offset = ContentClaimFieldMap.getContentClaimOffset(contentClaimRecord);
            if (offset != null) {
                builder.contentClaimOffset(offset);
            }
        }

        return builder.build();
    }
}
