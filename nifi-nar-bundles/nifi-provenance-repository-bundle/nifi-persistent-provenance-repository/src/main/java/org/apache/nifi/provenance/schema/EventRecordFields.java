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

package org.apache.nifi.provenance.schema;

import static org.apache.nifi.repository.schema.Repetition.EXACTLY_ONE;
import static org.apache.nifi.repository.schema.Repetition.ZERO_OR_MORE;
import static org.apache.nifi.repository.schema.Repetition.ZERO_OR_ONE;

import org.apache.nifi.repository.schema.ComplexRecordField;
import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.MapRecordField;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.SimpleRecordField;

public class EventRecordFields {

    // General Event fields.
    public static final RecordField RECORD_IDENTIFIER = new SimpleRecordField(EventFieldNames.EVENT_IDENTIFIER, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField EVENT_TYPE = new SimpleRecordField(EventFieldNames.EVENT_TYPE, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField EVENT_TIME = new SimpleRecordField(EventFieldNames.EVENT_TIME, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField FLOWFILE_ENTRY_DATE = new SimpleRecordField(EventFieldNames.FLOWFILE_ENTRY_DATE, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField EVENT_DURATION = new SimpleRecordField(EventFieldNames.EVENT_DURATION, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField LINEAGE_START_DATE = new SimpleRecordField(EventFieldNames.LINEAGE_START_DATE, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField COMPONENT_ID = new SimpleRecordField(EventFieldNames.COMPONENT_ID, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField COMPONENT_TYPE = new SimpleRecordField(EventFieldNames.COMPONENT_TYPE, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField FLOWFILE_UUID = new SimpleRecordField(EventFieldNames.FLOWFILE_UUID, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField EVENT_DETAILS = new SimpleRecordField(EventFieldNames.EVENT_DETAILS, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField SOURCE_QUEUE_IDENTIFIER = new SimpleRecordField(EventFieldNames.SOURCE_QUEUE_IDENTIFIER, FieldType.STRING, ZERO_OR_ONE);

    // Attributes
    public static final RecordField ATTRIBUTE_NAME = new SimpleRecordField(EventFieldNames.ATTRIBUTE_NAME, FieldType.LONG_STRING, EXACTLY_ONE);
    public static final RecordField ATTRIBUTE_VALUE_REQUIRED = new SimpleRecordField(EventFieldNames.ATTRIBUTE_VALUE, FieldType.LONG_STRING, EXACTLY_ONE);
    public static final RecordField ATTRIBUTE_VALUE_OPTIONAL = new SimpleRecordField(EventFieldNames.ATTRIBUTE_VALUE, FieldType.LONG_STRING, ZERO_OR_ONE);

    public static final RecordField PREVIOUS_ATTRIBUTES = new MapRecordField(EventFieldNames.PREVIOUS_ATTRIBUTES, ATTRIBUTE_NAME, ATTRIBUTE_VALUE_REQUIRED, EXACTLY_ONE);
    public static final RecordField UPDATED_ATTRIBUTES = new MapRecordField(EventFieldNames.UPDATED_ATTRIBUTES, ATTRIBUTE_NAME, ATTRIBUTE_VALUE_OPTIONAL, EXACTLY_ONE);

    // Content Claims
    public static final RecordField CONTENT_CLAIM_CONTAINER = new SimpleRecordField(EventFieldNames.CONTENT_CLAIM_CONTAINER, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_SECTION = new SimpleRecordField(EventFieldNames.CONTENT_CLAIM_SECTION, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_IDENTIFIER = new SimpleRecordField(EventFieldNames.CONTENT_CLAIM_IDENTIFIER, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_OFFSET = new SimpleRecordField(EventFieldNames.CONTENT_CLAIM_OFFSET, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_SIZE = new SimpleRecordField(EventFieldNames.CONTENT_CLAIM_SIZE, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField CURRENT_CONTENT_CLAIM = new ComplexRecordField(EventFieldNames.CONTENT_CLAIM, ZERO_OR_ONE,
        CONTENT_CLAIM_CONTAINER, CONTENT_CLAIM_SECTION, CONTENT_CLAIM_IDENTIFIER, CONTENT_CLAIM_OFFSET, CONTENT_CLAIM_SIZE);
    public static final RecordField PREVIOUS_CONTENT_CLAIM = new ComplexRecordField(EventFieldNames.PREVIOUS_CONTENT_CLAIM, ZERO_OR_ONE,
        CONTENT_CLAIM_CONTAINER, CONTENT_CLAIM_SECTION, CONTENT_CLAIM_IDENTIFIER, CONTENT_CLAIM_OFFSET, CONTENT_CLAIM_SIZE);

    // EventType-Specific fields
    // for FORK, JOIN, CLONE, REPLAY
    public static final RecordField PARENT_UUIDS = new SimpleRecordField(EventFieldNames.PARENT_UUIDS, FieldType.STRING, ZERO_OR_MORE);
    public static final RecordField CHILD_UUIDS = new SimpleRecordField(EventFieldNames.CHILD_UUIDS, FieldType.STRING, ZERO_OR_MORE);

    // for SEND/RECEIVE/FETCH
    public static final RecordField TRANSIT_URI = new SimpleRecordField(EventFieldNames.TRANSIT_URI, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField SOURCE_SYSTEM_FLOWFILE_IDENTIFIER = new SimpleRecordField(EventFieldNames.SOURCE_SYSTEM_FLOWFILE_IDENTIFIER, FieldType.STRING, ZERO_OR_ONE);

    // for ADD_INFO
    public static final RecordField ALTERNATE_IDENTIFIER = new SimpleRecordField(EventFieldNames.ALTERNATE_IDENTIFIER, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField RELATIONSHIP = new SimpleRecordField(EventFieldNames.RELATIONSHIP, FieldType.STRING, ZERO_OR_ONE);
}
