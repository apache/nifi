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

import org.apache.nifi.repository.schema.ComplexRecordField;
import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SimpleRecordField;
import org.apache.nifi.repository.schema.UnionRecordField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RepositoryRecordSchema {
    public static final String REPOSITORY_RECORD_UPDATE_V1 = "Repository Record Update";  // top level field name
    public static final String REPOSITORY_RECORD_UPDATE_V2 = "Repository Record Update";  // top level field name

    // repository record fields
    public static final String ACTION_TYPE = "Action";
    public static final String RECORD_ID = "Record ID";
    public static final String QUEUE_IDENTIFIER = "Queue Identifier";
    public static final String SWAP_LOCATION = "Swap Location";

    // Update types
    public static final String CREATE_OR_UPDATE_ACTION = "Create or Update";
    public static final String DELETE_ACTION = "Delete";
    public static final String SWAP_IN_ACTION = "Swap In";
    public static final String SWAP_OUT_ACTION = "Swap Out";

    public static final RecordSchema REPOSITORY_RECORD_SCHEMA_V1;
    public static final RecordSchema CREATE_OR_UPDATE_SCHEMA_V1;
    public static final RecordSchema DELETE_SCHEMA_V1;
    public static final RecordSchema SWAP_IN_SCHEMA_V1;
    public static final RecordSchema SWAP_OUT_SCHEMA_V1;

    public static final RecordSchema REPOSITORY_RECORD_SCHEMA_V2;
    public static final RecordSchema CREATE_OR_UPDATE_SCHEMA_V2;
    public static final RecordSchema DELETE_SCHEMA_V2;
    public static final RecordSchema SWAP_IN_SCHEMA_V2;
    public static final RecordSchema SWAP_OUT_SCHEMA_V2;

    public static final RecordField ACTION_TYPE_FIELD = new SimpleRecordField(ACTION_TYPE, FieldType.STRING, Repetition.EXACTLY_ONE);
    public static final RecordField RECORD_ID_FIELD = new SimpleRecordField(RECORD_ID, FieldType.LONG, Repetition.EXACTLY_ONE);

    static {
        // Fields for "Create" or "Update" records
        final List<RecordField> createOrUpdateFields = new ArrayList<>();
        createOrUpdateFields.add(ACTION_TYPE_FIELD);
        createOrUpdateFields.addAll(FlowFileSchema.FLOWFILE_SCHEMA_V1.getFields());

        createOrUpdateFields.add(new SimpleRecordField(QUEUE_IDENTIFIER, FieldType.STRING, Repetition.EXACTLY_ONE));
        createOrUpdateFields.add(new SimpleRecordField(SWAP_LOCATION, FieldType.STRING, Repetition.ZERO_OR_ONE));
        final ComplexRecordField createOrUpdate = new ComplexRecordField(CREATE_OR_UPDATE_ACTION, Repetition.EXACTLY_ONE, createOrUpdateFields);
        CREATE_OR_UPDATE_SCHEMA_V1 = new RecordSchema(createOrUpdateFields);

        // Fields for "Delete" records
        final List<RecordField> deleteFields = new ArrayList<>();
        deleteFields.add(ACTION_TYPE_FIELD);
        deleteFields.add(RECORD_ID_FIELD);
        final ComplexRecordField delete = new ComplexRecordField(DELETE_ACTION, Repetition.EXACTLY_ONE, deleteFields);
        DELETE_SCHEMA_V1 = new RecordSchema(deleteFields);

        // Fields for "Swap Out" records
        final List<RecordField> swapOutFields = new ArrayList<>();
        swapOutFields.add(ACTION_TYPE_FIELD);
        swapOutFields.add(RECORD_ID_FIELD);
        swapOutFields.add(new SimpleRecordField(QUEUE_IDENTIFIER, FieldType.STRING, Repetition.EXACTLY_ONE));
        swapOutFields.add(new SimpleRecordField(SWAP_LOCATION, FieldType.STRING, Repetition.EXACTLY_ONE));
        final ComplexRecordField swapOut = new ComplexRecordField(SWAP_OUT_ACTION, Repetition.EXACTLY_ONE, swapOutFields);
        SWAP_OUT_SCHEMA_V1 = new RecordSchema(swapOutFields);

        // Fields for "Swap In" records
        final List<RecordField> swapInFields = new ArrayList<>(createOrUpdateFields);
        swapInFields.add(new SimpleRecordField(SWAP_LOCATION, FieldType.STRING, Repetition.EXACTLY_ONE));
        final ComplexRecordField swapIn = new ComplexRecordField(SWAP_IN_ACTION, Repetition.EXACTLY_ONE, swapInFields);
        SWAP_IN_SCHEMA_V1 = new RecordSchema(swapInFields);

        // Union Field that creates the top-level field type
        final UnionRecordField repoUpdateField = new UnionRecordField(REPOSITORY_RECORD_UPDATE_V1, Repetition.EXACTLY_ONE, createOrUpdate, delete, swapOut, swapIn);
        REPOSITORY_RECORD_SCHEMA_V1 = new RecordSchema(Collections.singletonList(repoUpdateField));
    }

    static {
        // Fields for "Create" or "Update" records
        final List<RecordField> createOrUpdateFields = new ArrayList<>();
        createOrUpdateFields.add(ACTION_TYPE_FIELD);
        createOrUpdateFields.addAll(FlowFileSchema.FLOWFILE_SCHEMA_V2.getFields());

        createOrUpdateFields.add(new SimpleRecordField(QUEUE_IDENTIFIER, FieldType.STRING, Repetition.EXACTLY_ONE));
        createOrUpdateFields.add(new SimpleRecordField(SWAP_LOCATION, FieldType.STRING, Repetition.ZERO_OR_ONE));
        final ComplexRecordField createOrUpdate = new ComplexRecordField(CREATE_OR_UPDATE_ACTION, Repetition.EXACTLY_ONE, createOrUpdateFields);
        CREATE_OR_UPDATE_SCHEMA_V2 = new RecordSchema(createOrUpdateFields);

        // Fields for "Delete" records
        final List<RecordField> deleteFields = new ArrayList<>();
        deleteFields.add(ACTION_TYPE_FIELD);
        deleteFields.add(RECORD_ID_FIELD);
        final ComplexRecordField delete = new ComplexRecordField(DELETE_ACTION, Repetition.EXACTLY_ONE, deleteFields);
        DELETE_SCHEMA_V2 = new RecordSchema(deleteFields);

        // Fields for "Swap Out" records
        final List<RecordField> swapOutFields = new ArrayList<>();
        swapOutFields.add(ACTION_TYPE_FIELD);
        swapOutFields.add(RECORD_ID_FIELD);
        swapOutFields.add(new SimpleRecordField(QUEUE_IDENTIFIER, FieldType.STRING, Repetition.EXACTLY_ONE));
        swapOutFields.add(new SimpleRecordField(SWAP_LOCATION, FieldType.STRING, Repetition.EXACTLY_ONE));
        final ComplexRecordField swapOut = new ComplexRecordField(SWAP_OUT_ACTION, Repetition.EXACTLY_ONE, swapOutFields);
        SWAP_OUT_SCHEMA_V2 = new RecordSchema(swapOutFields);

        // Fields for "Swap In" records
        final List<RecordField> swapInFields = new ArrayList<>(createOrUpdateFields);
        swapInFields.add(new SimpleRecordField(SWAP_LOCATION, FieldType.STRING, Repetition.EXACTLY_ONE));
        final ComplexRecordField swapIn = new ComplexRecordField(SWAP_IN_ACTION, Repetition.EXACTLY_ONE, swapInFields);
        SWAP_IN_SCHEMA_V2 = new RecordSchema(swapInFields);

        // Union Field that creates the top-level field type
        final UnionRecordField repoUpdateField = new UnionRecordField(REPOSITORY_RECORD_UPDATE_V2, Repetition.EXACTLY_ONE, createOrUpdate, delete, swapOut, swapIn);
        REPOSITORY_RECORD_SCHEMA_V2 = new RecordSchema(Collections.singletonList(repoUpdateField));
    }
}
