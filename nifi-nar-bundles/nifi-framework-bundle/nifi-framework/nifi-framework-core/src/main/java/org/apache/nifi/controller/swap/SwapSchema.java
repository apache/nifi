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
import java.util.Collections;
import java.util.List;

import org.apache.nifi.controller.repository.schema.ContentClaimSchema;
import org.apache.nifi.controller.repository.schema.FlowFileSchema;
import org.apache.nifi.repository.schema.ComplexRecordField;
import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.MapRecordField;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SimpleRecordField;

public class SwapSchema {
    public static final RecordSchema SWAP_SUMMARY_SCHEMA_V1;
    public static final RecordSchema SWAP_CONTENTS_SCHEMA_V1;
    public static final RecordSchema FULL_SWAP_FILE_SCHEMA_V1;

    public static final RecordSchema SWAP_SUMMARY_SCHEMA_V2;
    public static final RecordSchema SWAP_CONTENTS_SCHEMA_V2;
    public static final RecordSchema FULL_SWAP_FILE_SCHEMA_V2;

    public static final String RESOURCE_CLAIMS = "Resource Claims";
    public static final String RESOURCE_CLAIM = "Resource Claim";
    public static final String RESOURCE_CLAIM_COUNT = "Claim Count";

    public static final String QUEUE_IDENTIFIER = "Queue Identifier";
    public static final String FLOWFILE_COUNT = "FlowFile Count";
    public static final String FLOWFILE_SIZE = "FlowFile Size";
    public static final String MAX_RECORD_ID = "Max Record ID";
    public static final String SWAP_SUMMARY = "Swap Summary";
    public static final String FLOWFILE_CONTENTS = "FlowFiles";

    static {
        final RecordField queueIdentifier = new SimpleRecordField(QUEUE_IDENTIFIER, FieldType.STRING, Repetition.EXACTLY_ONE);
        final RecordField flowFileCount = new SimpleRecordField(FLOWFILE_COUNT, FieldType.INT, Repetition.EXACTLY_ONE);
        final RecordField flowFileSize = new SimpleRecordField(FLOWFILE_SIZE, FieldType.LONG, Repetition.EXACTLY_ONE);
        final RecordField maxRecordId = new SimpleRecordField(MAX_RECORD_ID, FieldType.LONG, Repetition.EXACTLY_ONE);

        final RecordField resourceClaimField = new ComplexRecordField(RESOURCE_CLAIM, Repetition.EXACTLY_ONE, ContentClaimSchema.RESOURCE_CLAIM_SCHEMA_V1.getFields());
        final RecordField claimCountField = new SimpleRecordField(RESOURCE_CLAIM_COUNT, FieldType.INT, Repetition.EXACTLY_ONE);
        final RecordField resourceClaims = new MapRecordField(RESOURCE_CLAIMS, resourceClaimField, claimCountField, Repetition.EXACTLY_ONE);

        final List<RecordField> summaryFields = new ArrayList<>();
        summaryFields.add(queueIdentifier);
        summaryFields.add(flowFileCount);
        summaryFields.add(flowFileSize);
        summaryFields.add(maxRecordId);
        summaryFields.add(resourceClaims);
        SWAP_SUMMARY_SCHEMA_V1 = new RecordSchema(summaryFields);

        final RecordField flowFiles = new ComplexRecordField(FLOWFILE_CONTENTS, Repetition.ZERO_OR_MORE, FlowFileSchema.FLOWFILE_SCHEMA_V1.getFields());
        final List<RecordField> contentsFields = Collections.singletonList(flowFiles);
        SWAP_CONTENTS_SCHEMA_V1 = new RecordSchema(contentsFields);

        final List<RecordField> fullSchemaFields = new ArrayList<>();
        fullSchemaFields.add(new ComplexRecordField(SWAP_SUMMARY, Repetition.EXACTLY_ONE, summaryFields));
        fullSchemaFields.add(new ComplexRecordField(FLOWFILE_CONTENTS, Repetition.ZERO_OR_MORE, FlowFileSchema.FLOWFILE_SCHEMA_V1.getFields()));
        FULL_SWAP_FILE_SCHEMA_V1 = new RecordSchema(fullSchemaFields);
    }

    static {
        final RecordField queueIdentifier = new SimpleRecordField(QUEUE_IDENTIFIER, FieldType.STRING, Repetition.EXACTLY_ONE);
        final RecordField flowFileCount = new SimpleRecordField(FLOWFILE_COUNT, FieldType.INT, Repetition.EXACTLY_ONE);
        final RecordField flowFileSize = new SimpleRecordField(FLOWFILE_SIZE, FieldType.LONG, Repetition.EXACTLY_ONE);
        final RecordField maxRecordId = new SimpleRecordField(MAX_RECORD_ID, FieldType.LONG, Repetition.EXACTLY_ONE);

        final RecordField resourceClaimField = new ComplexRecordField(RESOURCE_CLAIM, Repetition.EXACTLY_ONE, ContentClaimSchema.RESOURCE_CLAIM_SCHEMA_V1.getFields());
        final RecordField claimCountField = new SimpleRecordField(RESOURCE_CLAIM_COUNT, FieldType.INT, Repetition.EXACTLY_ONE);
        final RecordField resourceClaims = new MapRecordField(RESOURCE_CLAIMS, resourceClaimField, claimCountField, Repetition.EXACTLY_ONE);

        final List<RecordField> summaryFields = new ArrayList<>();
        summaryFields.add(queueIdentifier);
        summaryFields.add(flowFileCount);
        summaryFields.add(flowFileSize);
        summaryFields.add(maxRecordId);
        summaryFields.add(resourceClaims);
        SWAP_SUMMARY_SCHEMA_V2 = new RecordSchema(summaryFields);

        final RecordField flowFiles = new ComplexRecordField(FLOWFILE_CONTENTS, Repetition.ZERO_OR_MORE, FlowFileSchema.FLOWFILE_SCHEMA_V2.getFields());
        final List<RecordField> contentsFields = Collections.singletonList(flowFiles);
        SWAP_CONTENTS_SCHEMA_V2 = new RecordSchema(contentsFields);

        final List<RecordField> fullSchemaFields = new ArrayList<>();
        fullSchemaFields.add(new ComplexRecordField(SWAP_SUMMARY, Repetition.EXACTLY_ONE, summaryFields));
        fullSchemaFields.add(new ComplexRecordField(FLOWFILE_CONTENTS, Repetition.ZERO_OR_MORE, FlowFileSchema.FLOWFILE_SCHEMA_V2.getFields()));
        FULL_SWAP_FILE_SCHEMA_V2 = new RecordSchema(fullSchemaFields);
    }
}
