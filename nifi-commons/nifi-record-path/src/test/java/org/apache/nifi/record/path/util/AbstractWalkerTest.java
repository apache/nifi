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
package org.apache.nifi.record.path.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * Abstract base class for tests that walk FieldValue hierarchies.
 */
public abstract class AbstractWalkerTest {

    protected List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("mapRecord", RecordFieldType.RECORD.getRecordDataType(getMapRecordSchema())));
        final DataType mapRecordDataType = RecordFieldType.RECORD.getRecordDataType(getMapRecordSchema());
        final DataType mapRecordArrayType = RecordFieldType.ARRAY.getArrayDataType(mapRecordDataType);
        final RecordField mapRecordArrayField = new RecordField("mapRecordArray", mapRecordArrayType);
        fields.add(mapRecordArrayField);
        return fields;
    }

    protected RecordSchema getMapRecordSchema() {
        final List<RecordField> mapRecordFields = new ArrayList<>();
        mapRecordFields
            .add(
                new RecordField("intArray", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
        return new SimpleRecordSchema(mapRecordFields);
    }

    protected void testNodeDepth(List<FieldValue> nodesToTest, int expectedPathDepth) {
        final List<FieldValue> callBackValues = new ArrayList<>();
        nodesToTest.forEach((value) -> FieldValueWalker.walk(value, callBackValues::add));
        assertEquals(nodesToTest.size() * expectedPathDepth, callBackValues.size());
    }

    protected Map<String, Object> buildMapRecord(int seed) {
        final Map<String, Object> mapRecordValues = new HashMap<>();
        mapRecordValues.put("intArray", new Object[] {seed});
        return mapRecordValues;
    }

    protected Object[] buildMapRecordArray(int seed, int count) {
        final ArrayList<MapRecord> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(new MapRecord(getMapRecordSchema(), buildMapRecord(seed)));
            seed++;
        }
        return list.toArray();
    }

    protected Record buildDefaultRecord() {
        final Map<String, Object> values = new HashMap<>();
        // map -> array[int]
        values.put("mapRecord", new MapRecord(getMapRecordSchema(), buildMapRecord(1)));
        // array[map -> array[int]]
        values.put("mapRecordArray", buildMapRecordArray(2, 4));
        return new MapRecord(new SimpleRecordSchema(getDefaultFields()), values);
    }

    protected FieldValueLogicalPathBuilder getDefaultLogicalPathBuilder() {
        return new FieldValueLogicalPathBuilder.Builder().build();
    }

    protected FieldValueLogicalPathBuilder getLogicalPathBuilder(CharSequence delimiter, CharSequence left,
        CharSequence right) {
        return new FieldValueLogicalPathBuilder.Builder()
            .withArrayIndexWrapperLeft(left)
            .withArrayIndexWrapperRight(right)
            .withMapKeyWrapperLeft(left)
            .withMapKeyWrapperRight(right)
            .withPathDelimiter(delimiter).build();
    }
}
