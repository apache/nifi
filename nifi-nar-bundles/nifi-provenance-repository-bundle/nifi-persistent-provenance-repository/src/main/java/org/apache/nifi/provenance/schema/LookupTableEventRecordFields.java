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
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SimpleRecordField;
import org.apache.nifi.repository.schema.UnionRecordField;

public class LookupTableEventRecordFields {

    // General Event fields.
    public static final RecordField RECORD_IDENTIFIER_OFFSET = new SimpleRecordField(EventFieldNames.EVENT_IDENTIFIER, FieldType.INT, EXACTLY_ONE);
    public static final RecordField EVENT_TYPE_ORDINAL = new SimpleRecordField(EventFieldNames.EVENT_TYPE, FieldType.INT, EXACTLY_ONE);
    public static final RecordField EVENT_TIME_OFFSET = new SimpleRecordField(EventFieldNames.EVENT_TIME, FieldType.INT, EXACTLY_ONE);
    public static final RecordField FLOWFILE_ENTRY_DATE_OFFSET = new SimpleRecordField(EventFieldNames.FLOWFILE_ENTRY_DATE, FieldType.INT, EXACTLY_ONE);
    public static final RecordField EVENT_DURATION = new SimpleRecordField(EventFieldNames.EVENT_DURATION, FieldType.INT, EXACTLY_ONE);
    public static final RecordField LINEAGE_START_DATE_OFFSET = new SimpleRecordField(EventFieldNames.LINEAGE_START_DATE, FieldType.INT, EXACTLY_ONE);
    public static final RecordField EVENT_DETAILS = new SimpleRecordField(EventFieldNames.EVENT_DETAILS, FieldType.STRING, ZERO_OR_ONE);

    // Make lookup id or a string, depending on whether or not available in header.
    public static final RecordField NO_VALUE = new SimpleRecordField(EventFieldNames.NO_VALUE, FieldType.STRING, Repetition.EXACTLY_ONE);
    public static final RecordField EXPLICIT_STRING = new SimpleRecordField(EventFieldNames.EXPLICIT_VALUE, FieldType.STRING, Repetition.EXACTLY_ONE);
    public static final RecordField LOOKUP_VALUE = new SimpleRecordField(EventFieldNames.LOOKUP_VALUE, FieldType.INT, Repetition.EXACTLY_ONE);
    public static final RecordField UNCHANGED_VALUE = new SimpleRecordField(EventFieldNames.UNCHANGED_VALUE, FieldType.STRING, Repetition.EXACTLY_ONE);

    public static final RecordField COMPONENT_ID = new UnionRecordField(EventFieldNames.COMPONENT_ID, Repetition.EXACTLY_ONE, NO_VALUE, EXPLICIT_STRING, LOOKUP_VALUE);
    public static final RecordField SOURCE_QUEUE_ID = new UnionRecordField(EventFieldNames.SOURCE_QUEUE_IDENTIFIER, Repetition.EXACTLY_ONE, NO_VALUE, EXPLICIT_STRING, LOOKUP_VALUE);
    public static final RecordField COMPONENT_TYPE = new UnionRecordField(EventFieldNames.COMPONENT_TYPE, Repetition.EXACTLY_ONE, EXPLICIT_STRING, LOOKUP_VALUE);

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

    public static final RecordField PREVIOUS_CONTENT_CLAIM = new ComplexRecordField(EventFieldNames.PREVIOUS_CONTENT_CLAIM, ZERO_OR_ONE,
            CONTENT_CLAIM_CONTAINER, CONTENT_CLAIM_SECTION, CONTENT_CLAIM_IDENTIFIER, CONTENT_CLAIM_OFFSET, CONTENT_CLAIM_SIZE);

    public static final RecordField CURRENT_CONTENT_CLAIM_EXPLICIT = new ComplexRecordField(EventFieldNames.EXPLICIT_VALUE, EXACTLY_ONE,
            CONTENT_CLAIM_CONTAINER, CONTENT_CLAIM_SECTION, CONTENT_CLAIM_IDENTIFIER, CONTENT_CLAIM_OFFSET, CONTENT_CLAIM_SIZE);
    public static final RecordField CURRENT_CONTENT_CLAIM = new UnionRecordField(EventFieldNames.CONTENT_CLAIM,
            Repetition.EXACTLY_ONE, NO_VALUE, UNCHANGED_VALUE, CURRENT_CONTENT_CLAIM_EXPLICIT);


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
