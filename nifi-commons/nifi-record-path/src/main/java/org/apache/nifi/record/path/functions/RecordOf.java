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
package org.apache.nifi.record.path.functions;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.nifi.serialization.record.util.DataTypeUtils.inferDataType;

public class RecordOf extends RecordPathSegment {
    private final RecordPathSegment[] valuePaths;

    public RecordOf(final RecordPathSegment[] valuePaths, final boolean absolute) {
        super("recordOf", null, absolute);
        this.valuePaths = valuePaths;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final List<RecordField> fields = new ArrayList<>();
        final Map<String, Object> values = new HashMap<>();

        for (int i = 0; i + 1 < valuePaths.length; i += 2) {
            final String fieldName = valuePaths[i].evaluate(context).findFirst().orElseThrow().toString();
            final FieldValue fieldValueProvider = valuePaths[i + 1].evaluate(context).findFirst().orElseThrow();

            final Object fieldValue = fieldValueProvider.getValue();

            final RecordField referencedField = fieldValueProvider.getField();
            final DataType fieldDataType = referencedField != null
                    ? referencedField.getDataType() : inferDataType(fieldValue, RecordFieldType.STRING.getDataType());

            fields.add(new RecordField(fieldName, fieldDataType));
            values.put(fieldName, fieldValue);
        }

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Record record = new MapRecord(schema, values);
        final RecordField recordField = new RecordField("recordOf", RecordFieldType.RECORD.getRecordDataType(schema));

        final FieldValue responseValue = new StandardFieldValue(record, recordField, null);
        return Stream.of(responseValue);
    }
}