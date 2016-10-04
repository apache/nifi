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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.schema.FlowFileRecordFieldMap;
import org.apache.nifi.repository.schema.ComplexRecordField;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SchemaRecordReader;

public class SchemaSwapDeserializer implements SwapDeserializer {

    @Override
    @SuppressWarnings("unchecked")
    public SwapContents deserializeFlowFiles(final DataInputStream in, final String swapLocation, final FlowFileQueue queue, final ResourceClaimManager claimManager) throws IOException {
        final RecordSchema schema = RecordSchema.readFrom(in);
        final SchemaRecordReader reader = SchemaRecordReader.fromSchema(schema);

        final Record parentRecord = reader.readRecord(in);
        final List<Record> flowFileRecords = (List<Record>) parentRecord.getFieldValue(SwapSchema.FLOWFILE_CONTENTS);

        final List<FlowFileRecord> flowFiles = new ArrayList<>(flowFileRecords.size());
        for (final Record record : flowFileRecords) {
            flowFiles.add(FlowFileRecordFieldMap.getFlowFile(record, claimManager));
        }

        final Record summaryRecord = (Record) parentRecord.getFieldValue(SwapSchema.SWAP_SUMMARY);
        final SwapSummary swapSummary = SwapSummaryFieldMap.getSwapSummary(summaryRecord, claimManager);

        return new StandardSwapContents(swapSummary, flowFiles);
    }

    @Override
    public SwapSummary getSwapSummary(final DataInputStream in, final String swapLocation, final ResourceClaimManager claimManager) throws IOException {
        final RecordSchema schema = RecordSchema.readFrom(in);
        final List<RecordField> summaryFields = schema.getField(SwapSchema.SWAP_SUMMARY).getSubFields();
        final RecordField summaryRecordField = new ComplexRecordField(SwapSchema.SWAP_SUMMARY, Repetition.EXACTLY_ONE, summaryFields);
        final RecordSchema summarySchema = new RecordSchema(Collections.singletonList(summaryRecordField));

        final Record summaryRecordParent = SchemaRecordReader.fromSchema(summarySchema).readRecord(in);
        final Record summaryRecord = (Record) summaryRecordParent.getFieldValue(SwapSchema.SWAP_SUMMARY);
        final SwapSummary swapSummary = SwapSummaryFieldMap.getSwapSummary(summaryRecord, claimManager);
        return swapSummary;
    }

    public static String getSerializationName() {
        return SchemaSwapSerializer.SERIALIZATION_NAME;
    }
}
