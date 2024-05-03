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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WrapperJoinStrategy extends IndexCorrelatedJoinStrategy {
    private static final String ORIGINAL_FIELD_NAME = "original";
    private static final String ENRICHMENT_FIELD_NAME = "enrichment";

    public WrapperJoinStrategy(final ComponentLog logger) {
        super(logger);
    }

    @Override
    protected Record combineRecords(final Record originalRecord, final Record enrichmentRecord, final RecordSchema resultSchema) {
        if (originalRecord == null && enrichmentRecord == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>();
        values.put(ORIGINAL_FIELD_NAME, originalRecord);
        values.put(ENRICHMENT_FIELD_NAME, enrichmentRecord);

        final Record wrapped = new MapRecord(resultSchema, values);
        return wrapped;
    }

    protected RecordSchema createResultSchema(final Record firstOriginalRecord, final Record firstEnrichmentRecord) {
        final List<RecordField> fields = new ArrayList<>();

        if (firstOriginalRecord != null) {
            fields.add(new RecordField(ORIGINAL_FIELD_NAME, RecordFieldType.RECORD.getRecordDataType(firstOriginalRecord.getSchema())));
        }

        if (firstEnrichmentRecord != null) {
            fields.add(new RecordField(ENRICHMENT_FIELD_NAME, RecordFieldType.RECORD.getRecordDataType(firstEnrichmentRecord.getSchema())));
        }

        final RecordSchema recordSetSchema = new SimpleRecordSchema(fields);
        return recordSetSchema;
    }

}
