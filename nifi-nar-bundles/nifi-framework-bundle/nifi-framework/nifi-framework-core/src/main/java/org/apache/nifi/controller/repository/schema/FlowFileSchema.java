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

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.repository.schema.ComplexRecordField;
import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.MapRecordField;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SimpleRecordField;

public class FlowFileSchema {

    public static final String RECORD_ID = "Record ID";
    public static final String ENTRY_DATE = "Entry Date";
    public static final String LINEAGE_START_DATE = "Lineage Start Date";
    public static final String LINEAGE_START_INDEX = "Lineage Start Index";
    public static final String QUEUE_DATE = "Queued Date";
    public static final String QUEUE_DATE_INDEX = "Queued Date Index";
    public static final String FLOWFILE_SIZE = "FlowFile Size";
    public static final String CONTENT_CLAIM = "Content Claim";
    public static final String ATTRIBUTES = "Attributes";

    // attribute fields
    public static final String ATTRIBUTE_NAME = "Attribute Name";
    public static final String ATTRIBUTE_VALUE = "Attribute Value";

    public static final RecordSchema FLOWFILE_SCHEMA_V1;
    public static final RecordSchema FLOWFILE_SCHEMA_V2;

    static {
        final List<RecordField> flowFileFields = new ArrayList<>();

        final RecordField attributeNameField = new SimpleRecordField(ATTRIBUTE_NAME, FieldType.STRING, Repetition.EXACTLY_ONE);
        final RecordField attributeValueField = new SimpleRecordField(ATTRIBUTE_VALUE, FieldType.STRING, Repetition.EXACTLY_ONE);

        flowFileFields.add(new SimpleRecordField(RECORD_ID, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(ENTRY_DATE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(LINEAGE_START_DATE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(LINEAGE_START_INDEX, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(QUEUE_DATE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(QUEUE_DATE_INDEX, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(FLOWFILE_SIZE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new ComplexRecordField(CONTENT_CLAIM, Repetition.ZERO_OR_ONE, ContentClaimSchema.CONTENT_CLAIM_SCHEMA_V1.getFields()));
        flowFileFields.add(new MapRecordField(ATTRIBUTES, attributeNameField, attributeValueField, Repetition.ZERO_OR_ONE));

        FLOWFILE_SCHEMA_V1 = new RecordSchema(flowFileFields);
    }

    static {
        final List<RecordField> flowFileFields = new ArrayList<>();

        final RecordField attributeNameField = new SimpleRecordField(ATTRIBUTE_NAME, FieldType.LONG_STRING, Repetition.EXACTLY_ONE);
        final RecordField attributeValueField = new SimpleRecordField(ATTRIBUTE_VALUE, FieldType.LONG_STRING, Repetition.EXACTLY_ONE);

        flowFileFields.add(new SimpleRecordField(RECORD_ID, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(ENTRY_DATE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(LINEAGE_START_DATE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(LINEAGE_START_INDEX, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(QUEUE_DATE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(QUEUE_DATE_INDEX, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new SimpleRecordField(FLOWFILE_SIZE, FieldType.LONG, Repetition.EXACTLY_ONE));
        flowFileFields.add(new ComplexRecordField(CONTENT_CLAIM, Repetition.ZERO_OR_ONE, ContentClaimSchema.CONTENT_CLAIM_SCHEMA_V1.getFields()));
        flowFileFields.add(new MapRecordField(ATTRIBUTES, attributeNameField, attributeValueField, Repetition.ZERO_OR_ONE));

        FLOWFILE_SCHEMA_V2 = new RecordSchema(flowFileFields);
    }
}
