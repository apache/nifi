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

import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.ALTERNATE_IDENTIFIER;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.CHILD_UUIDS;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.COMPONENT_ID;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.COMPONENT_TYPE;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.CURRENT_CONTENT_CLAIM;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.EVENT_DETAILS;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.EVENT_DURATION;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.EVENT_TIME_OFFSET;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.EVENT_TYPE_ORDINAL;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.EXPLICIT_STRING;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.FLOWFILE_ENTRY_DATE_OFFSET;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.LINEAGE_START_DATE_OFFSET;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.LOOKUP_VALUE;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.NO_VALUE;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.PARENT_UUIDS;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.PREVIOUS_ATTRIBUTES;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.PREVIOUS_CONTENT_CLAIM;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.RECORD_IDENTIFIER_OFFSET;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.RELATIONSHIP;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.SOURCE_QUEUE_ID;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.SOURCE_SYSTEM_FLOWFILE_IDENTIFIER;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.TRANSIT_URI;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.UNCHANGED_VALUE;
import static org.apache.nifi.provenance.schema.LookupTableEventRecordFields.UPDATED_ATTRIBUTES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;

public class LookupTableEventSchema {
    public static final RecordSchema EVENT_SCHEMA = buildSchemaV1(false);

    public static final RecordSchema NO_VALUE_SCHEMA = new RecordSchema(Collections.singletonList(NO_VALUE));
    public static final RecordSchema EXPLICIT_STRING_SCHEMA = new RecordSchema(Collections.singletonList(EXPLICIT_STRING));
    public static final RecordSchema UNCHANGED_VALUE_SCHEMA = new RecordSchema(Collections.singletonList(UNCHANGED_VALUE));
    public static final RecordSchema LOOKUP_VALUE_SCHEMA = new RecordSchema(Collections.singletonList(LOOKUP_VALUE));

    public static final RecordSchema CONTENT_CLAIM_SCHEMA = new RecordSchema(Collections.singletonList(CURRENT_CONTENT_CLAIM));

    private static RecordSchema buildSchemaV1(final boolean includeEventId) {
        final List<RecordField> fields = new ArrayList<>();
        if (includeEventId) {
            fields.add(RECORD_IDENTIFIER_OFFSET);
        }

        fields.add(EVENT_TYPE_ORDINAL);
        fields.add(EVENT_TIME_OFFSET);
        fields.add(FLOWFILE_ENTRY_DATE_OFFSET);
        fields.add(EVENT_DURATION);
        fields.add(LINEAGE_START_DATE_OFFSET);
        fields.add(COMPONENT_ID);
        fields.add(COMPONENT_TYPE);
        fields.add(EVENT_DETAILS);
        fields.add(PREVIOUS_ATTRIBUTES);
        fields.add(UPDATED_ATTRIBUTES);
        fields.add(CURRENT_CONTENT_CLAIM);
        fields.add(PREVIOUS_CONTENT_CLAIM);
        fields.add(SOURCE_QUEUE_ID);

        // EventType-Specific fields
        fields.add(PARENT_UUIDS);  // for FORK, JOIN, CLONE, REPLAY events
        fields.add(CHILD_UUIDS); // for FORK, JOIN, CLONE, REPLAY events
        fields.add(TRANSIT_URI); // for SEND/RECEIVE/FETCH events
        fields.add(SOURCE_SYSTEM_FLOWFILE_IDENTIFIER); // for SEND/RECEIVE events
        fields.add(ALTERNATE_IDENTIFIER); // for ADD_INFO events
        fields.add(RELATIONSHIP); // for ROUTE events

        final RecordSchema schema = new RecordSchema(fields);
        return schema;
    }
}
