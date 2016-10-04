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

    public static class Names {
        public static final String EVENT_IDENTIFIER = "Event ID";
        public static final String EVENT_TYPE = "Event Type";
        public static final String EVENT_TIME = "Event Time";
        public static final String FLOWFILE_ENTRY_DATE = "FlowFile Entry Date";
        public static final String EVENT_DURATION = "Event Duration";
        public static final String LINEAGE_START_DATE = "Lineage Start Date";
        public static final String COMPONENT_ID = "Component ID";
        public static final String COMPONENT_TYPE = "Component Type";
        public static final String FLOWFILE_UUID = "FlowFile UUID";
        public static final String EVENT_DETAILS = "Event Details";
        public static final String SOURCE_QUEUE_IDENTIFIER = "Source Queue Identifier";
        public static final String CONTENT_CLAIM = "Content Claim";
        public static final String PREVIOUS_CONTENT_CLAIM = "Previous Content Claim";
        public static final String PARENT_UUIDS = "Parent UUIDs";
        public static final String CHILD_UUIDS = "Child UUIDs";

        public static final String ATTRIBUTE_NAME = "Attribute Name";
        public static final String ATTRIBUTE_VALUE = "Attribute Value";
        public static final String PREVIOUS_ATTRIBUTES = "Previous Attributes";
        public static final String UPDATED_ATTRIBUTES = "Updated Attributes";

        public static final String CONTENT_CLAIM_CONTAINER = "Content Claim Container";
        public static final String CONTENT_CLAIM_SECTION = "Content Claim Section";
        public static final String CONTENT_CLAIM_IDENTIFIER = "Content Claim Identifier";
        public static final String CONTENT_CLAIM_OFFSET = "Content Claim Offset";
        public static final String CONTENT_CLAIM_SIZE = "Content Claim Size";

        public static final String TRANSIT_URI = "Transit URI";
        public static final String SOURCE_SYSTEM_FLOWFILE_IDENTIFIER = "Source System FlowFile Identifier";
        public static final String ALTERNATE_IDENTIFIER = "Alternate Identifier";
        public static final String RELATIONSHIP = "Relationship";
    }

    // General Event fields.
    public static final RecordField RECORD_IDENTIFIER = new SimpleRecordField(Names.EVENT_IDENTIFIER, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField EVENT_TYPE = new SimpleRecordField(Names.EVENT_TYPE, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField EVENT_TIME = new SimpleRecordField(Names.EVENT_TIME, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField FLOWFILE_ENTRY_DATE = new SimpleRecordField(Names.FLOWFILE_ENTRY_DATE, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField EVENT_DURATION = new SimpleRecordField(Names.EVENT_DURATION, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField LINEAGE_START_DATE = new SimpleRecordField(Names.LINEAGE_START_DATE, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField COMPONENT_ID = new SimpleRecordField(Names.COMPONENT_ID, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField COMPONENT_TYPE = new SimpleRecordField(Names.COMPONENT_TYPE, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField FLOWFILE_UUID = new SimpleRecordField(Names.FLOWFILE_UUID, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField EVENT_DETAILS = new SimpleRecordField(Names.EVENT_DETAILS, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField SOURCE_QUEUE_IDENTIFIER = new SimpleRecordField(Names.SOURCE_QUEUE_IDENTIFIER, FieldType.STRING, ZERO_OR_ONE);

    // Attributes
    public static final RecordField ATTRIBUTE_NAME = new SimpleRecordField(Names.ATTRIBUTE_NAME, FieldType.LONG_STRING, EXACTLY_ONE);
    public static final RecordField ATTRIBUTE_VALUE_REQUIRED = new SimpleRecordField(Names.ATTRIBUTE_VALUE, FieldType.LONG_STRING, EXACTLY_ONE);
    public static final RecordField ATTRIBUTE_VALUE_OPTIONAL = new SimpleRecordField(Names.ATTRIBUTE_VALUE, FieldType.LONG_STRING, ZERO_OR_ONE);

    public static final RecordField PREVIOUS_ATTRIBUTES = new MapRecordField(Names.PREVIOUS_ATTRIBUTES, ATTRIBUTE_NAME, ATTRIBUTE_VALUE_REQUIRED, EXACTLY_ONE);
    public static final RecordField UPDATED_ATTRIBUTES = new MapRecordField(Names.UPDATED_ATTRIBUTES, ATTRIBUTE_NAME, ATTRIBUTE_VALUE_OPTIONAL, EXACTLY_ONE);

    // Content Claims
    public static final RecordField CONTENT_CLAIM_CONTAINER = new SimpleRecordField(Names.CONTENT_CLAIM_CONTAINER, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_SECTION = new SimpleRecordField(Names.CONTENT_CLAIM_SECTION, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_IDENTIFIER = new SimpleRecordField(Names.CONTENT_CLAIM_IDENTIFIER, FieldType.STRING, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_OFFSET = new SimpleRecordField(Names.CONTENT_CLAIM_OFFSET, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField CONTENT_CLAIM_SIZE = new SimpleRecordField(Names.CONTENT_CLAIM_SIZE, FieldType.LONG, EXACTLY_ONE);
    public static final RecordField CURRENT_CONTENT_CLAIM = new ComplexRecordField(Names.CONTENT_CLAIM, ZERO_OR_ONE,
        CONTENT_CLAIM_CONTAINER, CONTENT_CLAIM_SECTION, CONTENT_CLAIM_IDENTIFIER, CONTENT_CLAIM_OFFSET, CONTENT_CLAIM_SIZE);
    public static final RecordField PREVIOUS_CONTENT_CLAIM = new ComplexRecordField(Names.PREVIOUS_CONTENT_CLAIM, ZERO_OR_ONE,
        CONTENT_CLAIM_CONTAINER, CONTENT_CLAIM_SECTION, CONTENT_CLAIM_IDENTIFIER, CONTENT_CLAIM_OFFSET, CONTENT_CLAIM_SIZE);

    // EventType-Specific fields
    // for FORK, JOIN, CLONE, REPLAY
    public static final RecordField PARENT_UUIDS = new SimpleRecordField(Names.PARENT_UUIDS, FieldType.STRING, ZERO_OR_MORE);
    public static final RecordField CHILD_UUIDS = new SimpleRecordField(Names.CHILD_UUIDS, FieldType.STRING, ZERO_OR_MORE);

    // for SEND/RECEIVE/FETCH
    public static final RecordField TRANSIT_URI = new SimpleRecordField(Names.TRANSIT_URI, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField SOURCE_SYSTEM_FLOWFILE_IDENTIFIER = new SimpleRecordField(Names.SOURCE_QUEUE_IDENTIFIER, FieldType.STRING, ZERO_OR_ONE);

    // for ADD_INFO
    public static final RecordField ALTERNATE_IDENTIFIER = new SimpleRecordField(Names.ALTERNATE_IDENTIFIER, FieldType.STRING, ZERO_OR_ONE);
    public static final RecordField RELATIONSHIP = new SimpleRecordField(Names.RELATIONSHIP, FieldType.STRING, ZERO_OR_ONE);
}
