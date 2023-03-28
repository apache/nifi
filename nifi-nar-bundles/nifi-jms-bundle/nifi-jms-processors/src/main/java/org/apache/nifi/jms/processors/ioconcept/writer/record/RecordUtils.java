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
package org.apache.nifi.jms.processors.ioconcept.writer.record;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordUtils {

    public static Record append(final Record originalRecord, final Map<String, String> decoratorValues, final String decoratorPrefix) {
        final List<RecordField> originalFields = originalRecord.getSchema().getFields();

        final List<RecordField> mergedFields = new ArrayList<>(originalFields);
        decoratorValues.forEach((key, value) -> mergedFields.add(new RecordField(decoratorPrefix + key, RecordFieldType.STRING.getDataType())));

        final RecordSchema mergedSchema = new SimpleRecordSchema(mergedFields);

        final Map<String, Object> recordValues = new HashMap<>();
        originalFields.stream().map(RecordField::getFieldName).forEach(fieldName -> recordValues.put(fieldName, originalRecord.getValue(fieldName)));
        decoratorValues.forEach((key, value) -> recordValues.put(decoratorPrefix + key, value));

        return new MapRecord(mergedSchema, recordValues);
    }

    public static MapRecord wrap(final Record originalRecord, final String originalRecordKey, final Map<String, String> decoratorValues, final String decoratorKey)
            throws IOException, MalformedRecordException {

        // create schema
        final Tuple<RecordField, Object> originalRecordLeaf = wrapStandardRecord(originalRecord, originalRecordKey);
        final Tuple<RecordField, Object> decoratorLeaf = wrapDecoratorValues(decoratorValues, decoratorKey);
        final RecordSchema rootRecordSchema = new SimpleRecordSchema(Arrays.asList(originalRecordLeaf.getKey(), decoratorLeaf.getKey()));

        // assign values
        final Map<String, Object> recordValues = new HashMap<>();
        recordValues.put(originalRecordLeaf.getKey().getFieldName(), originalRecordLeaf.getValue());
        recordValues.put(decoratorLeaf.getKey().getFieldName(), decoratorLeaf.getValue());
        return new MapRecord(rootRecordSchema, recordValues);
    }

    private static Tuple<RecordField, Object> wrapStandardRecord(final Record record, final String recordKey) {
        final RecordSchema recordSchema = (record == null) ? null : record.getSchema();
        final RecordField recordField = new RecordField(recordKey, RecordFieldType.RECORD.getRecordDataType(recordSchema));
        return new Tuple<>(recordField, record);
    }

    private static Tuple<RecordField, Object> wrapDecoratorValues(final Map<String, String> decoratorValues, final String decoratorKey) {
        final RecordField recordField = new RecordField(decoratorKey, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
        return new Tuple<>(recordField, decoratorValues);
    }

}
