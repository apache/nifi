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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.schema.FlowFileRecordFieldMap;
import org.apache.nifi.controller.repository.schema.FlowFileSchema;
import org.apache.nifi.repository.schema.ComplexRecordField;
import org.apache.nifi.repository.schema.FieldMapRecord;
import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SchemaRecordWriter;
import org.apache.nifi.repository.schema.SimpleRecordField;

public class SchemaSwapSerializer implements SwapSerializer {
    static final String SERIALIZATION_NAME = "Schema Swap Serialization";

    private final RecordSchema schema = SwapSchema.FULL_SWAP_FILE_SCHEMA_V2;
    private final RecordSchema flowFileSchema = new RecordSchema(schema.getField(SwapSchema.FLOWFILE_CONTENTS).getSubFields());

    @Override
    public void serializeFlowFiles(final List<FlowFileRecord> toSwap, final FlowFileQueue queue, final String swapLocation, final OutputStream out) throws IOException {
        schema.writeTo(out);

        long contentSize = 0L;
        long maxFlowFileId = -1L;
        final List<ResourceClaim> resourceClaims = new ArrayList<>();
        for (final FlowFileRecord flowFile : toSwap) {
            contentSize += flowFile.getSize();
            if (flowFile.getId() > maxFlowFileId) {
                maxFlowFileId = flowFile.getId();
            }

            final ContentClaim contentClaim = flowFile.getContentClaim();
            if (contentClaim != null) {
                resourceClaims.add(contentClaim.getResourceClaim());
            }
        }

        final QueueSize queueSize = new QueueSize(toSwap.size(), contentSize);
        final SwapSummary swapSummary = new StandardSwapSummary(queueSize, maxFlowFileId, resourceClaims);
        final Record summaryRecord = new SwapSummaryFieldMap(swapSummary, queue.getIdentifier(), SwapSchema.SWAP_SUMMARY_SCHEMA_V1);

        final List<Record> flowFileRecords = toSwap.stream()
            .map(flowFile -> new FlowFileRecordFieldMap(flowFile, flowFileSchema))
            .collect(Collectors.toList());

        // Create a simple record to hold the summary and the flowfile contents
        final RecordField summaryField = new SimpleRecordField(SwapSchema.SWAP_SUMMARY, FieldType.COMPLEX, Repetition.EXACTLY_ONE);
        final RecordField contentsField = new ComplexRecordField(SwapSchema.FLOWFILE_CONTENTS, Repetition.ZERO_OR_MORE, FlowFileSchema.FLOWFILE_SCHEMA_V2.getFields());
        final List<RecordField> fields = new ArrayList<>(2);
        fields.add(summaryField);
        fields.add(contentsField);

        final Map<RecordField, Object> swapFileMap = new LinkedHashMap<>();
        swapFileMap.put(summaryField, summaryRecord);
        swapFileMap.put(contentsField, flowFileRecords);
        final Record swapFileRecord = new FieldMapRecord(swapFileMap, new RecordSchema(fields));

        final SchemaRecordWriter writer = new SchemaRecordWriter();
        writer.writeRecord(swapFileRecord, out);
        out.flush();
    }

    @Override
    public String getSerializationName() {
        return SERIALIZATION_NAME;
    }

}
