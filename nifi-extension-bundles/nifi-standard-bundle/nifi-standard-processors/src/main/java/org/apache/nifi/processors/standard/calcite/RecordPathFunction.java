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

package org.apache.nifi.processors.standard.calcite;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class RecordPathFunction {
    private static final RecordField ROOT_RECORD_FIELD = new RecordField("root", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
    private static final RecordSchema ROOT_RECORD_SCHEMA = new SimpleRecordSchema(List.of(ROOT_RECORD_FIELD));
    private static final RecordField PARENT_RECORD_FIELD = new RecordField("root", RecordFieldType.RECORD.getRecordDataType(ROOT_RECORD_SCHEMA));

    protected static final RecordPathCache RECORD_PATH_CACHE = new RecordPathCache(100);

    protected <T> T eval(final Object record, final String recordPath, final Function<Object, T> transform) {
        if (record == null) {
            return null;
        }

        try {
            if (record instanceof Record) {
                return eval((Record) record, recordPath, transform);
            } else if (record instanceof Record[]) {
                return eval((Record[]) record, recordPath, transform);
            } else if (record instanceof Iterable) {
                return eval((Iterable<Record>) record, recordPath, transform);
            } else if (record instanceof Map) {
                return eval((Map<?, ?>) record, recordPath, transform);
            }
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " against " + record, e);
        }

        throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " against given argument because the argument is of type " + record.getClass() + " instead of Record");
    }

    private <T> T eval(final Map<?, ?> map, final String recordPath, final Function<Object, T> transform) {
        final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

        final Record record = new MapRecord(ROOT_RECORD_SCHEMA, Collections.singletonMap("root", map));
        final FieldValue parentFieldValue = new StandardFieldValue(record, PARENT_RECORD_FIELD, null);
        final FieldValue fieldValue = new StandardFieldValue(map, ROOT_RECORD_FIELD, parentFieldValue);
        final RecordPathResult result = compiled.evaluate(record, fieldValue);

        return evalResults(result.getSelectedFields(), transform, () -> "RecordPath " + recordPath + " resulted in more than one return value. The RecordPath must be further constrained.");
    }


    private <T> T eval(final Record record, final String recordPath, final Function<Object, T> transform) {
        final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);
        final RecordPathResult result = compiled.evaluate((Record) record);

        return evalResults(result.getSelectedFields(), transform,
            () -> "RecordPath " + recordPath + " evaluated against " + record + " resulted in more than one return value. The RecordPath must be further constrained.");
    }

    private <T> T eval(final Record[] records, final String recordPath, final Function<Object, T> transform) {
        final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

        final List<FieldValue> selectedFields = new ArrayList<>();
        for (final Record record : records) {
            final RecordPathResult result = compiled.evaluate(record);
            result.getSelectedFields().forEach(selectedFields::add);
        }

        if (selectedFields.isEmpty()) {
            return null;
        }

        return evalResults(selectedFields.stream(), transform, () -> "RecordPath " + recordPath + " resulted in more than one return value. The RecordPath must be further constrained.");
    }

    private <T> T  eval(final Iterable<Record> records, final String recordPath, final Function<Object, T> transform) {
        final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

        final List<FieldValue> selectedFields = new ArrayList<>();
        for (final Record record : records) {
            final RecordPathResult result = compiled.evaluate(record);
            result.getSelectedFields().forEach(selectedFields::add);
        }

        if (selectedFields.isEmpty()) {
            return null;
        }

        return evalResults(selectedFields.stream(), transform, () -> "RecordPath " + recordPath + " resulted in more than one return value. The RecordPath must be further constrained.");
    }


    private <T> T evalResults(final Stream<FieldValue> fields, final Function<Object, T> transform, final Supplier<String> multipleReturnValueErrorSupplier) {
        return fields.map(FieldValue::getValue)
            .filter(Objects::nonNull)
            .map(transform)
            .reduce((a, b) -> {
                // Only allow a single value
                throw new RuntimeException(multipleReturnValueErrorSupplier.get());
            })
            .orElse(null);

    }
}
