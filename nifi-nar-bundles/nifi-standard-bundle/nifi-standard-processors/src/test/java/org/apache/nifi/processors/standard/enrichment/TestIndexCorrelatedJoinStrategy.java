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

package org.apache.nifi.processors.standard.enrichment;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TestIndexCorrelatedJoinStrategy {
    protected RecordSchema getOriginalSchema() {
        final List<RecordField> originalFields = Collections.singletonList(new RecordField("id", RecordFieldType.INT.getDataType()));
        return new SimpleRecordSchema(originalFields);
    }

    protected RecordSchema getEnrichmentSchema() {
        final List<RecordField> enrichmentFields = Arrays.asList(
            new RecordField("name", RecordFieldType.STRING.getDataType()),
            new RecordField("number", RecordFieldType.INT.getDataType())
        );

        return new SimpleRecordSchema(enrichmentFields);
    }

    protected Record createOriginalRecord(final int id) {
        final RecordSchema originalSchema = getOriginalSchema();

        final Map<String, Object> originalValues = new HashMap<>();
        originalValues.put("id", id);
        return new MapRecord(originalSchema, originalValues);
    }

    protected Record createEnrichmentRecord(final int id, final String name, final int number) {
        final RecordSchema enrichmentSchema = getEnrichmentSchema();

        final Map<String, Object> enrichmentValues = new HashMap<>();
        enrichmentValues.put("id", id);
        enrichmentValues.put("name", name);
        enrichmentValues.put("number", number);

        final Record enrichmentRecord = new MapRecord(enrichmentSchema, enrichmentValues);
        return enrichmentRecord;
    }
}
