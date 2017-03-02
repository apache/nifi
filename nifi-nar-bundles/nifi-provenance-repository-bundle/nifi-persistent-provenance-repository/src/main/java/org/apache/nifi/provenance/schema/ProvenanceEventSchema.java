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

import static org.apache.nifi.provenance.schema.EventRecordFields.ALTERNATE_IDENTIFIER;
import static org.apache.nifi.provenance.schema.EventRecordFields.CHILD_UUIDS;
import static org.apache.nifi.provenance.schema.EventRecordFields.COMPONENT_ID;
import static org.apache.nifi.provenance.schema.EventRecordFields.COMPONENT_TYPE;
import static org.apache.nifi.provenance.schema.EventRecordFields.CURRENT_CONTENT_CLAIM;
import static org.apache.nifi.provenance.schema.EventRecordFields.EVENT_DETAILS;
import static org.apache.nifi.provenance.schema.EventRecordFields.EVENT_DURATION;
import static org.apache.nifi.provenance.schema.EventRecordFields.EVENT_TIME;
import static org.apache.nifi.provenance.schema.EventRecordFields.EVENT_TYPE;
import static org.apache.nifi.provenance.schema.EventRecordFields.FLOWFILE_ENTRY_DATE;
import static org.apache.nifi.provenance.schema.EventRecordFields.FLOWFILE_UUID;
import static org.apache.nifi.provenance.schema.EventRecordFields.LINEAGE_START_DATE;
import static org.apache.nifi.provenance.schema.EventRecordFields.PARENT_UUIDS;
import static org.apache.nifi.provenance.schema.EventRecordFields.PREVIOUS_ATTRIBUTES;
import static org.apache.nifi.provenance.schema.EventRecordFields.PREVIOUS_CONTENT_CLAIM;
import static org.apache.nifi.provenance.schema.EventRecordFields.RECORD_IDENTIFIER;
import static org.apache.nifi.provenance.schema.EventRecordFields.RELATIONSHIP;
import static org.apache.nifi.provenance.schema.EventRecordFields.SOURCE_QUEUE_IDENTIFIER;
import static org.apache.nifi.provenance.schema.EventRecordFields.SOURCE_SYSTEM_FLOWFILE_IDENTIFIER;
import static org.apache.nifi.provenance.schema.EventRecordFields.TRANSIT_URI;
import static org.apache.nifi.provenance.schema.EventRecordFields.UPDATED_ATTRIBUTES;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;

public class ProvenanceEventSchema {
    public static final RecordSchema PROVENANCE_EVENT_SCHEMA_V1 = buildSchemaV1(true);
    public static final RecordSchema PROVENANCE_EVENT_SCHEMA_V1_WITHOUT_EVENT_ID = buildSchemaV1(false);

    private static RecordSchema buildSchemaV1(final boolean includeEventId) {
        final List<RecordField> fields = new ArrayList<>();
        if (includeEventId) {
            fields.add(RECORD_IDENTIFIER);
        }

        fields.add(EVENT_TYPE);
        fields.add(EVENT_TIME);
        fields.add(FLOWFILE_ENTRY_DATE);
        fields.add(EVENT_DURATION);
        fields.add(LINEAGE_START_DATE);
        fields.add(COMPONENT_ID);
        fields.add(COMPONENT_TYPE);
        fields.add(FLOWFILE_UUID);
        fields.add(EVENT_DETAILS);
        fields.add(PREVIOUS_ATTRIBUTES);
        fields.add(UPDATED_ATTRIBUTES);
        fields.add(CURRENT_CONTENT_CLAIM);
        fields.add(PREVIOUS_CONTENT_CLAIM);
        fields.add(SOURCE_QUEUE_IDENTIFIER);

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
