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

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SimpleRecordField;

public class EventIdFirstHeaderSchema {

    public static RecordSchema SCHEMA = buildSchema();

    public static final class FieldNames {
        public static final String FIRST_EVENT_ID = "First Event ID";
        public static final String TIMESTAMP_OFFSET = "Timestamp Offset";
        public static final String COMPONENT_IDS = "Component Identifiers";
        public static final String COMPONENT_TYPES = "Component Types";
        public static final String QUEUE_IDS = "Queue Identifiers";
        public static final String EVENT_TYPES = "Event Types";
    }

    private static RecordSchema buildSchema() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new SimpleRecordField(FieldNames.FIRST_EVENT_ID, FieldType.LONG, Repetition.EXACTLY_ONE));
        fields.add(new SimpleRecordField(FieldNames.TIMESTAMP_OFFSET, FieldType.LONG, Repetition.EXACTLY_ONE));
        fields.add(new SimpleRecordField(FieldNames.COMPONENT_IDS, FieldType.STRING, Repetition.ZERO_OR_MORE));
        fields.add(new SimpleRecordField(FieldNames.COMPONENT_TYPES, FieldType.STRING, Repetition.ZERO_OR_MORE));
        fields.add(new SimpleRecordField(FieldNames.QUEUE_IDS, FieldType.STRING, Repetition.ZERO_OR_MORE));
        fields.add(new SimpleRecordField(FieldNames.EVENT_TYPES, FieldType.STRING, Repetition.ZERO_OR_MORE));
        return new RecordSchema(fields);
    }
}
